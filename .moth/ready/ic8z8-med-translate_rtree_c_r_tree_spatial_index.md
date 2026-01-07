# Translate rtree.c - R-Tree Spatial Index

## Overview
Translate R-tree spatial index extension for efficient spatial queries.

## Source Reference
- `sqlite3/ext/rtree/rtree.c` - R-tree implementation (4,485 lines)

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### R-Tree Table
```rust
/// R-tree virtual table
pub struct RtreeTable {
    /// Database connection
    db: *mut Connection,
    /// Table name
    name: String,
    /// Number of dimensions (1-5)
    n_dim: i32,
    /// Number of coordinate columns (n_dim * 2)
    n_coord: i32,
    /// Node page size
    node_size: i32,
    /// Node capacity
    node_capacity: i32,
    /// Shadow tables
    shadow: RtreeShadow,
}

struct RtreeShadow {
    /// _node table name
    node_table: String,
    /// _rowid table name
    rowid_table: String,
    /// _parent table name
    parent_table: String,
}

/// R-tree node
pub struct RtreeNode {
    /// Node ID
    id: i64,
    /// Is leaf node
    is_leaf: bool,
    /// Parent node
    parent: Option<i64>,
    /// Entries in this node
    entries: Vec<RtreeEntry>,
    /// Raw data (for serialization)
    data: Vec<u8>,
}

/// Entry in R-tree node
pub struct RtreeEntry {
    /// Row ID (for leaf) or child node ID (for internal)
    id: i64,
    /// Bounding box
    bbox: RtreeBbox,
}

/// Bounding box (supports 1-5 dimensions)
#[derive(Debug, Clone)]
pub struct RtreeBbox {
    /// Minimum coordinates
    min: Vec<f64>,
    /// Maximum coordinates
    max: Vec<f64>,
}

impl RtreeBbox {
    pub fn new(n_dim: usize) -> Self {
        Self {
            min: vec![f64::MAX; n_dim],
            max: vec![f64::MIN; n_dim],
        }
    }

    pub fn from_coords(coords: &[f64]) -> Self {
        let n_dim = coords.len() / 2;
        let mut bbox = Self::new(n_dim);

        for i in 0..n_dim {
            bbox.min[i] = coords[i * 2];
            bbox.max[i] = coords[i * 2 + 1];
        }

        bbox
    }

    /// Check if bounding boxes overlap
    pub fn overlaps(&self, other: &RtreeBbox) -> bool {
        for i in 0..self.min.len() {
            if self.max[i] < other.min[i] || self.min[i] > other.max[i] {
                return false;
            }
        }
        true
    }

    /// Check if this bbox contains other
    pub fn contains(&self, other: &RtreeBbox) -> bool {
        for i in 0..self.min.len() {
            if self.min[i] > other.min[i] || self.max[i] < other.max[i] {
                return false;
            }
        }
        true
    }

    /// Expand bbox to include other
    pub fn expand(&mut self, other: &RtreeBbox) {
        for i in 0..self.min.len() {
            self.min[i] = self.min[i].min(other.min[i]);
            self.max[i] = self.max[i].max(other.max[i]);
        }
    }

    /// Calculate area (or hypervolume for n-dim)
    pub fn area(&self) -> f64 {
        let mut area = 1.0;
        for i in 0..self.min.len() {
            area *= self.max[i] - self.min[i];
        }
        area
    }

    /// Calculate overlap area with another bbox
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
```

### R-Tree Cursor
```rust
/// R-tree cursor for queries
pub struct RtreeCursor {
    /// Table reference
    table: Arc<RtreeTable>,
    /// Query constraint
    constraint: Option<RtreeConstraint>,
    /// Search stack
    stack: Vec<SearchStackEntry>,
    /// Current result
    current: Option<RtreeResult>,
    /// Is at EOF
    eof: bool,
}

struct SearchStackEntry {
    node_id: i64,
    entry_idx: usize,
}

#[derive(Debug, Clone)]
pub enum RtreeConstraint {
    /// Overlap with query box
    Overlap(RtreeBbox),
    /// Within query box
    Within(RtreeBbox),
    /// Contains point
    ContainsPoint(Vec<f64>),
    /// Custom constraint function
    Custom(RtreeGeometry),
}

pub struct RtreeResult {
    /// Row ID
    pub rowid: i64,
    /// Bounding box
    pub bbox: RtreeBbox,
}
```

### R-Tree Geometry Callbacks
```rust
/// Custom geometry callback for R-tree queries
pub struct RtreeGeometry {
    /// Geometry data
    data: Vec<u8>,
    /// Test function
    test_fn: GeometryTestFn,
    /// Score function (optional, for nearest neighbor)
    score_fn: Option<GeometryScoreFn>,
}

pub type GeometryTestFn = fn(&RtreeGeometry, &RtreeBbox) -> RtreeTestResult;
pub type GeometryScoreFn = fn(&RtreeGeometry, &RtreeBbox) -> f64;

#[derive(Debug, Clone, Copy)]
pub enum RtreeTestResult {
    /// Definitely not in result set
    NotWithin,
    /// Partially in result set (need to check children)
    Partial,
    /// Fully in result set
    Within,
}
```

## R-Tree Operations

### Insert
```rust
impl RtreeTable {
    /// Insert a new entry
    pub fn insert(&mut self, rowid: i64, coords: &[f64]) -> Result<()> {
        let bbox = RtreeBbox::from_coords(coords);
        let entry = RtreeEntry { id: rowid, bbox: bbox.clone() };

        // Find best leaf node
        let leaf_id = self.choose_leaf(&bbox)?;
        let mut leaf = self.load_node(leaf_id)?;

        // Insert into leaf
        leaf.entries.push(entry);

        if leaf.entries.len() > self.node_capacity as usize {
            // Node overflow - split
            self.split_node(leaf)?;
        } else {
            self.save_node(&leaf)?;
            // Adjust ancestors
            self.adjust_tree(leaf_id)?;
        }

        // Record in rowid table
        self.insert_rowid(rowid, leaf_id)?;

        Ok(())
    }

    /// Choose best leaf for insertion
    fn choose_leaf(&self, bbox: &RtreeBbox) -> Result<i64> {
        let mut node_id = self.root_id()?;

        loop {
            let node = self.load_node(node_id)?;

            if node.is_leaf {
                return Ok(node_id);
            }

            // Find entry with minimum enlargement
            let mut best_idx = 0;
            let mut best_enlargement = f64::MAX;
            let mut best_area = f64::MAX;

            for (i, entry) in node.entries.iter().enumerate() {
                let mut expanded = entry.bbox.clone();
                expanded.expand(bbox);
                let enlargement = expanded.area() - entry.bbox.area();

                if enlargement < best_enlargement ||
                   (enlargement == best_enlargement && entry.bbox.area() < best_area) {
                    best_idx = i;
                    best_enlargement = enlargement;
                    best_area = entry.bbox.area();
                }
            }

            node_id = node.entries[best_idx].id;
        }
    }

    /// Split an overflowing node
    fn split_node(&mut self, mut node: RtreeNode) -> Result<()> {
        // Use quadratic split algorithm
        let (group1, group2) = self.quadratic_split(&node.entries);

        // Create new node
        let new_id = self.allocate_node()?;
        let mut new_node = RtreeNode {
            id: new_id,
            is_leaf: node.is_leaf,
            parent: node.parent,
            entries: group2,
            data: Vec::new(),
        };

        node.entries = group1;

        // Save both nodes
        self.save_node(&node)?;
        self.save_node(&new_node)?;

        // Update parent
        if let Some(parent_id) = node.parent {
            self.update_parent(parent_id, node.id, &new_node)?;
        } else {
            // Split root - create new root
            self.create_new_root(&node, &new_node)?;
        }

        Ok(())
    }

    fn quadratic_split(&self, entries: &[RtreeEntry]) -> (Vec<RtreeEntry>, Vec<RtreeEntry>) {
        // Pick seeds - entries with maximum waste
        let (seed1, seed2) = self.pick_seeds(entries);

        let mut group1 = vec![entries[seed1].clone()];
        let mut group2 = vec![entries[seed2].clone()];
        let mut bbox1 = entries[seed1].bbox.clone();
        let mut bbox2 = entries[seed2].bbox.clone();

        let mut assigned = vec![false; entries.len()];
        assigned[seed1] = true;
        assigned[seed2] = true;

        // Assign remaining entries
        for _ in 2..entries.len() {
            let min_size = (entries.len() + 1) / 2;

            // Check if one group needs all remaining
            let remaining: Vec<_> = (0..entries.len())
                .filter(|&i| !assigned[i])
                .collect();

            if group1.len() + remaining.len() == min_size {
                for i in remaining {
                    group1.push(entries[i].clone());
                }
                break;
            }
            if group2.len() + remaining.len() == min_size {
                for i in remaining {
                    group2.push(entries[i].clone());
                }
                break;
            }

            // Pick next entry
            let (idx, prefer_group1) = self.pick_next(&entries, &assigned, &bbox1, &bbox2);
            assigned[idx] = true;

            if prefer_group1 {
                bbox1.expand(&entries[idx].bbox);
                group1.push(entries[idx].clone());
            } else {
                bbox2.expand(&entries[idx].bbox);
                group2.push(entries[idx].clone());
            }
        }

        (group1, group2)
    }
}
```

### Query
```rust
impl RtreeCursor {
    /// Execute spatial query
    pub fn query(&mut self, constraint: RtreeConstraint) -> Result<()> {
        self.constraint = Some(constraint);
        self.stack.clear();

        // Start at root
        let root_id = self.table.root_id()?;
        self.stack.push(SearchStackEntry {
            node_id: root_id,
            entry_idx: 0,
        });

        // Find first result
        self.find_next()?;

        Ok(())
    }

    fn find_next(&mut self) -> Result<()> {
        while let Some(entry) = self.stack.last_mut() {
            let node = self.table.load_node(entry.node_id)?;

            while entry.entry_idx < node.entries.len() {
                let idx = entry.entry_idx;
                entry.entry_idx += 1;

                let rtree_entry = &node.entries[idx];

                if self.test_constraint(&rtree_entry.bbox) {
                    if node.is_leaf {
                        // Found result
                        self.current = Some(RtreeResult {
                            rowid: rtree_entry.id,
                            bbox: rtree_entry.bbox.clone(),
                        });
                        return Ok(());
                    } else {
                        // Descend into child
                        self.stack.push(SearchStackEntry {
                            node_id: rtree_entry.id,
                            entry_idx: 0,
                        });
                        return self.find_next();
                    }
                }
            }

            // Exhausted this node
            self.stack.pop();
        }

        // No more results
        self.current = None;
        self.eof = true;
        Ok(())
    }

    fn test_constraint(&self, bbox: &RtreeBbox) -> bool {
        match &self.constraint {
            Some(RtreeConstraint::Overlap(query)) => bbox.overlaps(query),
            Some(RtreeConstraint::Within(query)) => query.contains(bbox),
            Some(RtreeConstraint::ContainsPoint(point)) => {
                for i in 0..point.len() {
                    if point[i] < bbox.min[i] || point[i] > bbox.max[i] {
                        return false;
                    }
                }
                true
            }
            Some(RtreeConstraint::Custom(geom)) => {
                matches!((geom.test_fn)(geom, bbox), RtreeTestResult::Partial | RtreeTestResult::Within)
            }
            None => true,
        }
    }
}
```

### Delete
```rust
impl RtreeTable {
    /// Delete an entry
    pub fn delete(&mut self, rowid: i64) -> Result<()> {
        // Find node containing entry
        let node_id = self.find_node_for_rowid(rowid)?;
        let mut node = self.load_node(node_id)?;

        // Remove entry
        node.entries.retain(|e| e.id != rowid);

        if node.entries.len() < self.min_entries() {
            // Underflow - condense tree
            self.condense_tree(node)?;
        } else {
            self.save_node(&node)?;
            self.adjust_tree(node_id)?;
        }

        // Remove from rowid table
        self.delete_rowid(rowid)?;

        Ok(())
    }

    fn condense_tree(&mut self, mut node: RtreeNode) -> Result<()> {
        let mut orphans = Vec::new();

        let mut current = node;
        while let Some(parent_id) = current.parent {
            if current.entries.len() < self.min_entries() {
                // Collect orphan entries
                orphans.extend(current.entries.drain(..));

                // Remove from parent
                let mut parent = self.load_node(parent_id)?;
                parent.entries.retain(|e| e.id != current.id);
                self.delete_node(current.id)?;

                current = parent;
            } else {
                self.save_node(&current)?;
                break;
            }
        }

        // Reinsert orphans
        for entry in orphans {
            if current.is_leaf {
                // Entry is a data entry
                // Need to get coords from somewhere
            } else {
                // Entry is a node - reinsert at appropriate level
                self.reinsert_node(entry)?;
            }
        }

        Ok(())
    }
}
```

## Virtual Table Implementation
```rust
impl VirtualTable for RtreeTable {
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Check for spatial constraints
        let mut has_constraint = false;

        for i in 0..info.constraint_count() {
            let constraint = info.constraint(i)?;
            if constraint.column >= 1 && constraint.column <= self.n_coord {
                // Coordinate constraint
                has_constraint = true;
                info.set_constraint_usage(i, true, false)?;
            }
        }

        if has_constraint {
            info.estimated_cost = 30.0;
        } else {
            info.estimated_cost = 1000000.0; // Full scan
        }

        Ok(())
    }

    fn open(&self) -> Result<Box<dyn Cursor>> {
        Ok(Box::new(RtreeCursor {
            table: Arc::new(self.clone()),
            constraint: None,
            stack: Vec::new(),
            current: None,
            eof: false,
        }))
    }
}
```

## Acceptance Criteria
- [ ] R-tree virtual table creation
- [ ] Shadow tables (_node, _rowid, _parent)
- [ ] 1-5 dimensional support
- [ ] Insert operation
- [ ] Delete operation
- [ ] Overlap queries
- [ ] Within queries
- [ ] Contains point queries
- [ ] Custom geometry callbacks
- [ ] Node splitting (quadratic algorithm)
- [ ] Tree condensing on delete
- [ ] Nearest neighbor queries (optional)

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `rtree.test` - Core R-tree functionality
- `rtree1.test` - R-tree basic operations
- `rtree2.test` - R-tree queries
- `rtree3.test` - R-tree insert/delete
- `rtree4.test` - R-tree constraints
- `rtree5.test` - R-tree geometry callbacks
- `rtree6.test` - R-tree multiple dimensions
- `rtree7.test` - R-tree edge cases
- `rtree8.test` - R-tree performance
- `rtree9.test` - R-tree with transactions
- `rtreeA.test` - R-tree auxiliary columns
- `rtreeB.test` - R-tree boundary conditions
- `rtreeC.test` - R-tree corruption handling
- `rtreeD.test` - R-tree nearest neighbor
- `rtreeE.test` - R-tree expression handling
- `rtreeF.test` - R-tree fault injection
- `rtreeG.test` - R-tree geometry functions
- `rtreefault.test` - R-tree error handling
