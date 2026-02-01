//! R-Tree spatial index (in-memory implementation)
//!
//! This module provides a simplified, SQLite-compatible R-Tree data structure
//! intended to mirror the rtree.c extension logic in a Rust-friendly form.

use std::collections::HashMap;

use crate::error::{Error, ErrorCode, Result};

pub const RTREE_MAX_DIMENSIONS: usize = 5;
pub const RTREE_DEFAULT_NODE_CAPACITY: usize = 16;

#[derive(Debug, Clone)]
pub struct RtreeBbox {
    pub min: Vec<f64>,
    pub max: Vec<f64>,
}

impl RtreeBbox {
    pub fn new(n_dim: usize) -> Result<Self> {
        if n_dim == 0 || n_dim > RTREE_MAX_DIMENSIONS {
            return Err(Error::with_message(
                ErrorCode::Error,
                "invalid dimension count",
            ));
        }
        Ok(Self {
            min: vec![f64::MAX; n_dim],
            max: vec![f64::MIN; n_dim],
        })
    }

    pub fn from_coords(coords: &[f64]) -> Result<Self> {
        if !coords.len().is_multiple_of(2) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "coordinate list must be min/max pairs",
            ));
        }
        let n_dim = coords.len() / 2;
        let mut bbox = Self::new(n_dim)?;
        for i in 0..n_dim {
            bbox.min[i] = coords[i * 2];
            bbox.max[i] = coords[i * 2 + 1];
        }
        Ok(bbox)
    }

    pub fn overlaps(&self, other: &RtreeBbox) -> bool {
        for i in 0..self.min.len() {
            if self.max[i] < other.min[i] || self.min[i] > other.max[i] {
                return false;
            }
        }
        true
    }

    pub fn contains(&self, other: &RtreeBbox) -> bool {
        for i in 0..self.min.len() {
            if self.min[i] > other.min[i] || self.max[i] < other.max[i] {
                return false;
            }
        }
        true
    }

    pub fn expand(&mut self, other: &RtreeBbox) {
        for i in 0..self.min.len() {
            self.min[i] = self.min[i].min(other.min[i]);
            self.max[i] = self.max[i].max(other.max[i]);
        }
    }

    pub fn area(&self) -> f64 {
        let mut area = 1.0;
        for i in 0..self.min.len() {
            area *= self.max[i] - self.min[i];
        }
        area
    }

    pub fn overlap_area(&self, other: &RtreeBbox) -> f64 {
        let mut area = 1.0;
        for i in 0..self.min.len() {
            let overlap = self.max[i].min(other.max[i]) - self.min[i].max(other.min[i]);
            if overlap <= 0.0 {
                return 0.0;
            }
            area *= overlap;
        }
        area
    }
}

#[derive(Debug, Clone)]
pub struct RtreeEntry {
    pub id: i64,
    pub bbox: RtreeBbox,
}

#[derive(Debug, Clone)]
pub struct RtreeNode {
    pub id: i64,
    pub is_leaf: bool,
    pub parent: Option<i64>,
    pub entries: Vec<RtreeEntry>,
}

#[derive(Debug, Clone)]
pub struct RtreeResult {
    pub rowid: i64,
    pub bbox: RtreeBbox,
}

#[derive(Debug, Clone)]
pub enum RtreeConstraint {
    Overlap(RtreeBbox),
    Within(RtreeBbox),
    ContainsPoint(Vec<f64>),
}

pub struct RtreeTable {
    pub n_dim: usize,
    pub n_coord: usize,
    pub node_capacity: usize,
    root_id: i64,
    nodes: HashMap<i64, RtreeNode>,
    rowid_map: HashMap<i64, i64>,
    next_node_id: i64,
}

impl RtreeTable {
    pub fn new(n_dim: usize, node_capacity: usize) -> Result<Self> {
        if n_dim == 0 || n_dim > RTREE_MAX_DIMENSIONS {
            return Err(Error::with_message(
                ErrorCode::Error,
                "invalid dimension count",
            ));
        }
        let capacity = if node_capacity == 0 {
            RTREE_DEFAULT_NODE_CAPACITY
        } else {
            node_capacity
        };
        let root_id = 1;
        let root = RtreeNode {
            id: root_id,
            is_leaf: true,
            parent: None,
            entries: Vec::new(),
        };
        let mut nodes = HashMap::new();
        nodes.insert(root_id, root);
        Ok(Self {
            n_dim,
            n_coord: n_dim * 2,
            node_capacity: capacity,
            root_id,
            nodes,
            rowid_map: HashMap::new(),
            next_node_id: root_id + 1,
        })
    }

    pub fn insert(&mut self, rowid: i64, coords: &[f64]) -> Result<()> {
        let bbox = RtreeBbox::from_coords(coords)?;
        if bbox.min.len() != self.n_dim {
            return Err(Error::with_message(ErrorCode::Error, "dimension mismatch"));
        }
        let leaf_id = self.choose_leaf(&bbox)?;
        let entry = RtreeEntry { id: rowid, bbox };
        self.insert_entry(leaf_id, entry)?;
        self.rowid_map.insert(rowid, leaf_id);
        Ok(())
    }

    pub fn delete(&mut self, rowid: i64) -> Result<()> {
        let leaf_id = match self.rowid_map.remove(&rowid) {
            Some(id) => id,
            None => return Ok(()),
        };
        if let Some(node) = self.nodes.get_mut(&leaf_id) {
            node.entries.retain(|entry| entry.id != rowid);
        }
        self.condense_tree(leaf_id)?;
        Ok(())
    }

    pub fn query(&self, constraint: RtreeConstraint) -> Vec<RtreeResult> {
        let mut results = Vec::new();
        let mut stack = vec![self.root_id];
        while let Some(node_id) = stack.pop() {
            let node = match self.nodes.get(&node_id) {
                Some(node) => node,
                None => continue,
            };
            for entry in &node.entries {
                if !self.test_constraint(&constraint, &entry.bbox) {
                    continue;
                }
                if node.is_leaf {
                    results.push(RtreeResult {
                        rowid: entry.id,
                        bbox: entry.bbox.clone(),
                    });
                } else {
                    stack.push(entry.id);
                }
            }
        }
        results
    }

    /// Get an entry by rowid (O(1) lookup via rowid_map)
    pub fn get(&self, rowid: i64) -> Option<RtreeResult> {
        let leaf_id = self.rowid_map.get(&rowid)?;
        let node = self.nodes.get(leaf_id)?;
        for entry in &node.entries {
            if entry.id == rowid {
                return Some(RtreeResult {
                    rowid: entry.id,
                    bbox: entry.bbox.clone(),
                });
            }
        }
        None
    }

    fn test_constraint(&self, constraint: &RtreeConstraint, bbox: &RtreeBbox) -> bool {
        match constraint {
            RtreeConstraint::Overlap(query) => bbox.overlaps(query),
            RtreeConstraint::Within(query) => query.contains(bbox),
            RtreeConstraint::ContainsPoint(point) => {
                if point.len() != self.n_dim {
                    return false;
                }
                for i in 0..self.n_dim {
                    if point[i] < bbox.min[i] || point[i] > bbox.max[i] {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn choose_leaf(&self, bbox: &RtreeBbox) -> Result<i64> {
        let mut node_id = self.root_id;
        loop {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
            if node.is_leaf {
                return Ok(node_id);
            }
            let mut best_id = None;
            let mut best_enlargement = f64::MAX;
            let mut best_area = f64::MAX;
            for entry in &node.entries {
                let mut expanded = entry.bbox.clone();
                expanded.expand(bbox);
                let enlargement = expanded.area() - entry.bbox.area();
                if enlargement < best_enlargement
                    || (enlargement == best_enlargement && entry.bbox.area() < best_area)
                {
                    best_enlargement = enlargement;
                    best_area = entry.bbox.area();
                    best_id = Some(entry.id);
                }
            }
            node_id = best_id.ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
        }
    }

    fn insert_entry(&mut self, node_id: i64, entry: RtreeEntry) -> Result<()> {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.entries.push(entry);
        }
        if self
            .nodes
            .get(&node_id)
            .map(|node| node.entries.len() > self.node_capacity)
            .unwrap_or(false)
        {
            self.split_node(node_id)?;
        } else {
            self.adjust_tree(node_id)?;
        }
        Ok(())
    }

    fn split_node(&mut self, node_id: i64) -> Result<()> {
        let node = self
            .nodes
            .remove(&node_id)
            .ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
        let is_leaf = node.is_leaf;
        let parent_id = node.parent;
        let entries = node.entries;

        let (group1, group2) = self.quadratic_split(&entries)?;

        let node1 = RtreeNode {
            id: node_id,
            is_leaf,
            parent: parent_id,
            entries: group1,
        };
        let node2_id = self.next_node_id;
        self.next_node_id += 1;
        let node2 = RtreeNode {
            id: node2_id,
            is_leaf,
            parent: parent_id,
            entries: group2,
        };
        self.nodes.insert(node1.id, node1);
        self.nodes.insert(node2.id, node2);

        if let Some(parent_id) = parent_id {
            self.replace_parent_entry(parent_id, node_id, node2_id)?;
            self.adjust_tree(parent_id)?;
        } else {
            let new_root_id = self.next_node_id;
            self.next_node_id += 1;
            let mut new_root = RtreeNode {
                id: new_root_id,
                is_leaf: false,
                parent: None,
                entries: Vec::new(),
            };
            let bbox1 = self.node_bbox(node_id)?;
            let bbox2 = self.node_bbox(node2_id)?;
            new_root.entries.push(RtreeEntry {
                id: node_id,
                bbox: bbox1,
            });
            new_root.entries.push(RtreeEntry {
                id: node2_id,
                bbox: bbox2,
            });
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.parent = Some(new_root_id);
            }
            if let Some(node) = self.nodes.get_mut(&node2_id) {
                node.parent = Some(new_root_id);
            }
            self.nodes.insert(new_root_id, new_root);
            self.root_id = new_root_id;
        }

        Ok(())
    }

    fn quadratic_split(
        &self,
        entries: &[RtreeEntry],
    ) -> Result<(Vec<RtreeEntry>, Vec<RtreeEntry>)> {
        if entries.len() < 2 {
            return Err(Error::with_message(
                ErrorCode::Error,
                "split needs >=2 entries",
            ));
        }
        let (seed1, seed2) = self.pick_seeds(entries);
        let mut group1 = vec![entries[seed1].clone()];
        let mut group2 = vec![entries[seed2].clone()];
        let mut bbox1 = entries[seed1].bbox.clone();
        let mut bbox2 = entries[seed2].bbox.clone();

        let mut assigned = vec![false; entries.len()];
        assigned[seed1] = true;
        assigned[seed2] = true;

        let min_size = self.node_capacity.div_ceil(2);

        for _ in 2..entries.len() {
            let remaining: Vec<usize> = (0..entries.len()).filter(|i| !assigned[*i]).collect();
            if group1.len() + remaining.len() == min_size {
                for idx in remaining {
                    group1.push(entries[idx].clone());
                }
                break;
            }
            if group2.len() + remaining.len() == min_size {
                for idx in remaining {
                    group2.push(entries[idx].clone());
                }
                break;
            }

            let (idx, prefer_group1) = self.pick_next(entries, &assigned, &bbox1, &bbox2);
            assigned[idx] = true;
            if prefer_group1 {
                bbox1.expand(&entries[idx].bbox);
                group1.push(entries[idx].clone());
            } else {
                bbox2.expand(&entries[idx].bbox);
                group2.push(entries[idx].clone());
            }
        }

        Ok((group1, group2))
    }

    fn pick_seeds(&self, entries: &[RtreeEntry]) -> (usize, usize) {
        let mut best_waste = f64::MIN;
        let mut seed1 = 0;
        let mut seed2 = 1;
        for i in 0..entries.len() {
            for j in (i + 1)..entries.len() {
                let mut combined = entries[i].bbox.clone();
                combined.expand(&entries[j].bbox);
                let waste = combined.area() - entries[i].bbox.area() - entries[j].bbox.area();
                if waste > best_waste {
                    best_waste = waste;
                    seed1 = i;
                    seed2 = j;
                }
            }
        }
        (seed1, seed2)
    }

    fn pick_next(
        &self,
        entries: &[RtreeEntry],
        assigned: &[bool],
        bbox1: &RtreeBbox,
        bbox2: &RtreeBbox,
    ) -> (usize, bool) {
        let mut best_idx = 0;
        let mut best_diff = f64::MIN;
        let mut prefer_group1 = true;
        for (idx, entry) in entries.iter().enumerate() {
            if assigned[idx] {
                continue;
            }
            let mut expanded1 = bbox1.clone();
            expanded1.expand(&entry.bbox);
            let mut expanded2 = bbox2.clone();
            expanded2.expand(&entry.bbox);
            let e1 = expanded1.area() - bbox1.area();
            let e2 = expanded2.area() - bbox2.area();
            let diff = (e1 - e2).abs();
            if diff > best_diff {
                best_diff = diff;
                best_idx = idx;
                prefer_group1 = if e1 < e2 {
                    true
                } else if e2 < e1 {
                    false
                } else {
                    bbox1.area() < bbox2.area()
                };
            }
        }
        (best_idx, prefer_group1)
    }

    fn node_bbox(&self, node_id: i64) -> Result<RtreeBbox> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
        let mut bbox = RtreeBbox::new(self.n_dim)?;
        for entry in &node.entries {
            bbox.expand(&entry.bbox);
        }
        Ok(bbox)
    }

    fn replace_parent_entry(
        &mut self,
        parent_id: i64,
        old_child: i64,
        new_child: i64,
    ) -> Result<()> {
        let bbox_old = self.node_bbox(old_child)?;
        let bbox_new = self.node_bbox(new_child)?;
        if let Some(parent) = self.nodes.get_mut(&parent_id) {
            for entry in &mut parent.entries {
                if entry.id == old_child {
                    entry.bbox = bbox_old.clone();
                }
            }
            parent.entries.push(RtreeEntry {
                id: new_child,
                bbox: bbox_new,
            });
        }
        if self
            .nodes
            .get(&parent_id)
            .map(|node| node.entries.len() > self.node_capacity)
            .unwrap_or(false)
        {
            self.split_node(parent_id)?;
        }
        Ok(())
    }

    fn adjust_tree(&mut self, mut node_id: i64) -> Result<()> {
        loop {
            let parent_id = match self.nodes.get(&node_id).and_then(|node| node.parent) {
                Some(id) => id,
                None => break,
            };
            let bbox = self.node_bbox(node_id)?;
            if let Some(parent) = self.nodes.get_mut(&parent_id) {
                for entry in &mut parent.entries {
                    if entry.id == node_id {
                        entry.bbox = bbox.clone();
                        break;
                    }
                }
            }
            node_id = parent_id;
        }
        Ok(())
    }

    fn condense_tree(&mut self, mut node_id: i64) -> Result<()> {
        let min_entries = self.node_capacity.div_ceil(2);
        let mut orphan_entries = Vec::new();

        loop {
            let parent_id = match self.nodes.get(&node_id).and_then(|node| node.parent) {
                Some(id) => id,
                None => break,
            };
            let node_entries = self
                .nodes
                .get(&node_id)
                .map(|node| node.entries.len())
                .unwrap_or(0);
            if node_entries < min_entries {
                if let Some(node) = self.nodes.remove(&node_id) {
                    if node.is_leaf {
                        orphan_entries.extend(node.entries);
                    } else {
                        for entry in node.entries {
                            orphan_entries.push(entry);
                        }
                    }
                }
                if let Some(parent) = self.nodes.get_mut(&parent_id) {
                    parent.entries.retain(|entry| entry.id != node_id);
                }
            } else {
                self.adjust_tree(node_id)?;
            }
            node_id = parent_id;
        }

        if let Some(root) = self.nodes.get(&self.root_id) {
            if !root.is_leaf && root.entries.len() == 1 {
                let child_id = root.entries[0].id;
                if let Some(child) = self.nodes.get_mut(&child_id) {
                    child.parent = None;
                }
                self.nodes.remove(&self.root_id);
                self.root_id = child_id;
            }
        }

        for entry in orphan_entries {
            if let Some(node) = self.nodes.get(&entry.id) {
                if !node.is_leaf {
                    self.insert_subtree(entry.id)?;
                    continue;
                }
            }
            self.insert(entry.id, &self.coords_from_bbox(&entry.bbox)?)?;
        }

        Ok(())
    }

    fn insert_subtree(&mut self, node_id: i64) -> Result<()> {
        let depth = self.node_depth(node_id)?;
        let target_id = self.choose_subtree(self.root_id, depth)?;
        let bbox = self.node_bbox(node_id)?;
        if let Some(target) = self.nodes.get_mut(&target_id) {
            target.entries.push(RtreeEntry { id: node_id, bbox });
        }
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.parent = Some(target_id);
        }
        if self
            .nodes
            .get(&target_id)
            .map(|node| node.entries.len() > self.node_capacity)
            .unwrap_or(false)
        {
            self.split_node(target_id)?;
        } else {
            self.adjust_tree(target_id)?;
        }
        Ok(())
    }

    fn node_depth(&self, mut node_id: i64) -> Result<usize> {
        let mut depth = 0usize;
        while let Some(node) = self.nodes.get(&node_id) {
            if let Some(parent) = node.parent {
                depth += 1;
                node_id = parent;
            } else {
                break;
            }
        }
        Ok(depth)
    }

    fn choose_subtree(&self, mut node_id: i64, target_depth: usize) -> Result<i64> {
        let mut depth = self.node_depth(node_id)?;
        while depth > target_depth {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
            let mut best_id = None;
            let mut best_area = f64::MAX;
            for entry in &node.entries {
                let area = entry.bbox.area();
                if area < best_area {
                    best_area = area;
                    best_id = Some(entry.id);
                }
            }
            node_id = best_id.ok_or_else(|| Error::new(ErrorCode::Corrupt))?;
            depth -= 1;
        }
        Ok(node_id)
    }

    fn coords_from_bbox(&self, bbox: &RtreeBbox) -> Result<Vec<f64>> {
        if bbox.min.len() != self.n_dim || bbox.max.len() != self.n_dim {
            return Err(Error::with_message(ErrorCode::Error, "dimension mismatch"));
        }
        let mut coords = Vec::with_capacity(self.n_coord);
        for i in 0..self.n_dim {
            coords.push(bbox.min[i]);
            coords.push(bbox.max[i]);
        }
        Ok(coords)
    }

    // ========================================================================
    // Serialization for Shadow Table Persistence
    // ========================================================================

    /// Get the tree depth (height from root to leaves)
    pub fn tree_depth(&self) -> i32 {
        let mut depth = 0i32;
        let mut node_id = self.root_id;
        while let Some(node) = self.nodes.get(&node_id) {
            if node.is_leaf {
                break;
            }
            if let Some(first_entry) = node.entries.first() {
                depth += 1;
                node_id = first_entry.id;
            } else {
                break;
            }
        }
        depth
    }

    /// Get root node ID
    pub fn root_id(&self) -> i64 {
        self.root_id
    }

    /// Serialize a node to SQLite R-tree format
    ///
    /// Format:
    /// - 2 bytes: tree depth (for root only, 0 for non-root)
    /// - 2 bytes: number of entries (big-endian u16)
    /// - For each entry:
    ///   - 8 bytes: rowid/child_id (big-endian i64)
    ///   - 4 * n_coord bytes: coordinates as big-endian f32
    ///
    /// Note: Output is padded to SQLite's fixed node size (RTREE_MAXCELLS=51)
    pub fn serialize_node(&self, node_id: i64) -> Result<Vec<u8>> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "node not found"))?;

        // SQLite uses fixed-size nodes: 4-byte header + MAXCELLS * bytes_per_cell
        // RTREE_MAXCELLS = 51, bytes_per_cell = 8 + 8*n_dim
        const RTREE_MAXCELLS: usize = 51;
        let bytes_per_cell = 8 + 8 * self.n_dim;
        let node_size = 4 + RTREE_MAXCELLS * bytes_per_cell;

        let mut data = vec![0u8; node_size];

        // 2 bytes: tree depth (only meaningful for root)
        let depth = if node_id == self.root_id {
            self.tree_depth() as u16
        } else {
            0u16
        };
        data[0..2].copy_from_slice(&depth.to_be_bytes());

        // 2 bytes: number of entries
        let n_entries = node.entries.len() as u16;
        data[2..4].copy_from_slice(&n_entries.to_be_bytes());

        // Each entry
        let mut offset = 4;
        for entry in &node.entries {
            // 8 bytes: rowid or child node id
            data[offset..offset + 8].copy_from_slice(&entry.id.to_be_bytes());
            offset += 8;

            // Coordinates as f32 (4 bytes each)
            for i in 0..self.n_dim {
                let min_f32 = entry.bbox.min[i] as f32;
                let max_f32 = entry.bbox.max[i] as f32;
                data[offset..offset + 4].copy_from_slice(&min_f32.to_be_bytes());
                offset += 4;
                data[offset..offset + 4].copy_from_slice(&max_f32.to_be_bytes());
                offset += 4;
            }
        }

        Ok(data)
    }

    /// Deserialize a node from SQLite R-tree format
    ///
    /// Returns (is_leaf, entries)
    /// Note: is_leaf is determined by whether depth==0 and node has no children
    pub fn deserialize_node(&self, data: &[u8], is_leaf: bool) -> Result<Vec<RtreeEntry>> {
        if data.len() < 4 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "node data too short",
            ));
        }

        let bytes_per_cell = 8 + 4 * self.n_coord;

        // Skip 2 bytes depth, read 2 bytes entry count
        let n_entries = u16::from_be_bytes([data[2], data[3]]) as usize;

        let expected_len = 4 + n_entries * bytes_per_cell;
        if data.len() < expected_len {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "node data truncated",
            ));
        }

        let mut entries = Vec::with_capacity(n_entries);
        let mut offset = 4;

        for _ in 0..n_entries {
            // 8 bytes rowid
            let id = i64::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            // Coordinates
            let mut min = Vec::with_capacity(self.n_dim);
            let mut max = Vec::with_capacity(self.n_dim);
            for _ in 0..self.n_dim {
                let min_f32 = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                let max_f32 = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                min.push(min_f32 as f64);
                max.push(max_f32 as f64);
            }

            entries.push(RtreeEntry {
                id,
                bbox: RtreeBbox { min, max },
            });
        }

        Ok(entries)
    }

    /// Get all node IDs in the tree
    pub fn all_node_ids(&self) -> Vec<i64> {
        self.nodes.keys().copied().collect()
    }

    /// Get all rowid to node mappings
    pub fn all_rowid_mappings(&self) -> Vec<(i64, i64)> {
        self.rowid_map.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// Get parent node ID for a given node
    pub fn get_parent(&self, node_id: i64) -> Option<i64> {
        self.nodes.get(&node_id).and_then(|n| n.parent)
    }

    /// Check if a node is a leaf
    pub fn is_leaf(&self, node_id: i64) -> bool {
        self.nodes.get(&node_id).map(|n| n.is_leaf).unwrap_or(false)
    }

    /// Load R-tree from shadow table data
    ///
    /// This reconstructs the in-memory R-tree from persisted shadow table data.
    pub fn load_from_shadow_data(
        n_dim: usize,
        node_capacity: usize,
        node_data: Vec<(i64, Vec<u8>)>, // (nodeno, blob)
        rowid_data: Vec<(i64, i64)>,    // (rowid, nodeno)
        parent_data: Vec<(i64, i64)>,   // (nodeno, parentnode)
    ) -> Result<Self> {
        if n_dim == 0 || n_dim > RTREE_MAX_DIMENSIONS {
            return Err(Error::with_message(
                ErrorCode::Error,
                "invalid dimension count",
            ));
        }
        let capacity = if node_capacity == 0 {
            RTREE_DEFAULT_NODE_CAPACITY
        } else {
            node_capacity
        };

        // Build parent lookup
        let parent_map: HashMap<i64, i64> = parent_data.into_iter().collect();

        // Determine root and tree structure from depth in node 1
        let root_id = 1i64;
        let mut max_node_id = root_id;

        // First, get the tree depth from root node
        let tree_depth = if let Some((_, data)) = node_data.iter().find(|(id, _)| *id == root_id) {
            if data.len() >= 2 {
                u16::from_be_bytes([data[0], data[1]]) as usize
            } else {
                0
            }
        } else {
            0
        };

        // Create empty table
        let n_coord = n_dim * 2;
        let mut nodes = HashMap::new();
        let rowid_map: HashMap<i64, i64> = rowid_data.into_iter().collect();

        // Parse all nodes
        for (node_id, data) in node_data {
            max_node_id = max_node_id.max(node_id);

            // Determine if leaf based on depth from root
            // Leaves are at depth == tree_depth
            let node_depth = Self::compute_node_depth(node_id, &parent_map);
            let is_leaf = node_depth == tree_depth;

            // Parse entries
            let bytes_per_cell = 8 + 4 * n_coord;
            if data.len() < 4 {
                continue;
            }
            let n_entries = u16::from_be_bytes([data[2], data[3]]) as usize;

            let mut entries = Vec::with_capacity(n_entries);
            let mut offset = 4;

            for _ in 0..n_entries {
                if offset + bytes_per_cell > data.len() {
                    break;
                }

                let id = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;

                let mut min = Vec::with_capacity(n_dim);
                let mut max = Vec::with_capacity(n_dim);
                for _ in 0..n_dim {
                    let min_f32 = f32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    offset += 4;
                    let max_f32 = f32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    offset += 4;
                    min.push(min_f32 as f64);
                    max.push(max_f32 as f64);
                }

                entries.push(RtreeEntry {
                    id,
                    bbox: RtreeBbox { min, max },
                });
            }

            let parent = parent_map.get(&node_id).copied();

            nodes.insert(
                node_id,
                RtreeNode {
                    id: node_id,
                    is_leaf,
                    parent,
                    entries,
                },
            );
        }

        // If no nodes were loaded, create empty tree with root
        if nodes.is_empty() {
            let root = RtreeNode {
                id: root_id,
                is_leaf: true,
                parent: None,
                entries: Vec::new(),
            };
            nodes.insert(root_id, root);
        }

        Ok(Self {
            n_dim,
            n_coord,
            node_capacity: capacity,
            root_id,
            nodes,
            rowid_map,
            next_node_id: max_node_id + 1,
        })
    }

    /// Compute depth of a node from root using parent chain
    fn compute_node_depth(node_id: i64, parent_map: &HashMap<i64, i64>) -> usize {
        let mut depth = 0;
        let mut current = node_id;
        while let Some(&parent) = parent_map.get(&current) {
            if parent == 0 {
                break;
            }
            depth += 1;
            current = parent;
        }
        depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox_overlap_contains() {
        let a = RtreeBbox::from_coords(&[0.0, 5.0, 0.0, 5.0]).unwrap();
        let b = RtreeBbox::from_coords(&[4.0, 6.0, 4.0, 6.0]).unwrap();
        let c = RtreeBbox::from_coords(&[6.0, 7.0, 6.0, 7.0]).unwrap();
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
        assert!(a.contains(&a));
        assert!(!a.contains(&b));
    }

    #[test]
    fn test_insert_query() {
        let mut table = RtreeTable::new(2, 4).unwrap();
        table.insert(1, &[0.0, 1.0, 0.0, 1.0]).unwrap();
        table.insert(2, &[2.0, 3.0, 2.0, 3.0]).unwrap();
        let query = RtreeBbox::from_coords(&[0.5, 2.5, 0.5, 2.5]).unwrap();
        let results = table.query(RtreeConstraint::Overlap(query));
        let ids: Vec<i64> = results.iter().map(|r| r.rowid).collect();
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn test_delete() {
        let mut table = RtreeTable::new(2, 4).unwrap();
        table.insert(1, &[0.0, 1.0, 0.0, 1.0]).unwrap();
        table.insert(2, &[2.0, 3.0, 2.0, 3.0]).unwrap();
        table.delete(1).unwrap();
        let query = RtreeBbox::from_coords(&[0.0, 4.0, 0.0, 4.0]).unwrap();
        let results = table.query(RtreeConstraint::Overlap(query));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].rowid, 2);
    }
}
