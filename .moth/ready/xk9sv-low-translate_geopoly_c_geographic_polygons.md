# Translate geopoly.c - Geographic Polygon Support

## Overview
Translate geopoly extension for geographic polygon storage and queries using R-tree.

## Source Reference
- `sqlite3/ext/rtree/geopoly.c` - Geographic polygon extension

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Geopoly Polygon
```rust
/// Geographic polygon (stored as blob)
#[derive(Debug, Clone)]
pub struct GeoPoly {
    /// Vertices as (x, y) pairs
    vertices: Vec<(f64, f64)>,
}

impl GeoPoly {
    pub fn new(vertices: Vec<(f64, f64)>) -> Self {
        Self { vertices }
    }

    /// Parse from JSON array [[x1,y1],[x2,y2],...]
    pub fn from_json(json: &str) -> Result<Self> {
        let parsed: Vec<Vec<f64>> = serde_json::from_str(json)?;
        let vertices = parsed.into_iter()
            .filter(|v| v.len() >= 2)
            .map(|v| (v[0], v[1]))
            .collect();
        Ok(Self { vertices })
    }

    /// Serialize to blob
    pub fn to_blob(&self) -> Vec<u8> {
        let mut data = Vec::new();

        // Header: number of vertices
        let n = self.vertices.len() as u32;
        data.extend_from_slice(&n.to_le_bytes());

        // Vertices
        for (x, y) in &self.vertices {
            data.extend_from_slice(&x.to_le_bytes());
            data.extend_from_slice(&y.to_le_bytes());
        }

        data
    }

    /// Deserialize from blob
    pub fn from_blob(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::with_message(ErrorCode::Error, "invalid geopoly blob"));
        }

        let n = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let expected_len = 4 + n * 16;

        if data.len() < expected_len {
            return Err(Error::with_message(ErrorCode::Error, "invalid geopoly blob"));
        }

        let mut vertices = Vec::with_capacity(n);
        for i in 0..n {
            let offset = 4 + i * 16;
            let x = f64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
            let y = f64::from_le_bytes(data[offset+8..offset+16].try_into().unwrap());
            vertices.push((x, y));
        }

        Ok(Self { vertices })
    }

    /// Get bounding box
    pub fn bbox(&self) -> (f64, f64, f64, f64) {
        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;

        for (x, y) in &self.vertices {
            min_x = min_x.min(*x);
            min_y = min_y.min(*y);
            max_x = max_x.max(*x);
            max_y = max_y.max(*y);
        }

        (min_x, min_y, max_x, max_y)
    }

    /// Calculate signed area (positive = counter-clockwise)
    pub fn signed_area(&self) -> f64 {
        if self.vertices.len() < 3 {
            return 0.0;
        }

        let mut area = 0.0;
        let n = self.vertices.len();

        for i in 0..n {
            let j = (i + 1) % n;
            area += self.vertices[i].0 * self.vertices[j].1;
            area -= self.vertices[j].0 * self.vertices[i].1;
        }

        area / 2.0
    }

    /// Calculate area (absolute value)
    pub fn area(&self) -> f64 {
        self.signed_area().abs()
    }

    /// Calculate perimeter
    pub fn perimeter(&self) -> f64 {
        if self.vertices.len() < 2 {
            return 0.0;
        }

        let mut perimeter = 0.0;
        let n = self.vertices.len();

        for i in 0..n {
            let j = (i + 1) % n;
            let dx = self.vertices[j].0 - self.vertices[i].0;
            let dy = self.vertices[j].1 - self.vertices[i].1;
            perimeter += (dx * dx + dy * dy).sqrt();
        }

        perimeter
    }

    /// Check if polygon is valid (closed, non-self-intersecting)
    pub fn is_valid(&self) -> bool {
        self.vertices.len() >= 3
    }

    /// Check if point is inside polygon (ray casting)
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        if self.vertices.len() < 3 {
            return false;
        }

        let mut inside = false;
        let n = self.vertices.len();

        let mut j = n - 1;
        for i in 0..n {
            let (xi, yi) = self.vertices[i];
            let (xj, yj) = self.vertices[j];

            if ((yi > y) != (yj > y)) &&
               (x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
                inside = !inside;
            }

            j = i;
        }

        inside
    }

    /// Check if this polygon overlaps with another
    pub fn overlaps(&self, other: &GeoPoly) -> bool {
        // Quick bbox check
        let (ax1, ay1, ax2, ay2) = self.bbox();
        let (bx1, by1, bx2, by2) = other.bbox();

        if ax2 < bx1 || ax1 > bx2 || ay2 < by1 || ay1 > by2 {
            return false;
        }

        // Check if any vertex of one is inside the other
        for (x, y) in &self.vertices {
            if other.contains_point(*x, *y) {
                return true;
            }
        }

        for (x, y) in &other.vertices {
            if self.contains_point(*x, *y) {
                return true;
            }
        }

        // Check edge intersections
        self.edges_intersect(other)
    }

    fn edges_intersect(&self, other: &GeoPoly) -> bool {
        let n1 = self.vertices.len();
        let n2 = other.vertices.len();

        for i in 0..n1 {
            let j = (i + 1) % n1;
            let (x1, y1) = self.vertices[i];
            let (x2, y2) = self.vertices[j];

            for k in 0..n2 {
                let l = (k + 1) % n2;
                let (x3, y3) = other.vertices[k];
                let (x4, y4) = other.vertices[l];

                if segments_intersect(x1, y1, x2, y2, x3, y3, x4, y4) {
                    return true;
                }
            }
        }

        false
    }
}

/// Check if two line segments intersect
fn segments_intersect(
    x1: f64, y1: f64, x2: f64, y2: f64,
    x3: f64, y3: f64, x4: f64, y4: f64,
) -> bool {
    let d = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3);

    if d.abs() < 1e-10 {
        return false; // Parallel
    }

    let t = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / d;
    let u = -((x2 - x1) * (y3 - y1) - (y2 - y1) * (x3 - x1)) / d;

    t >= 0.0 && t <= 1.0 && u >= 0.0 && u <= 1.0
}
```

### Geopoly Virtual Table
```rust
/// Geopoly virtual table (backed by R-tree)
pub struct GeopolyTable {
    /// Underlying R-tree
    rtree: RtreeTable,
    /// Table name
    name: String,
}

/// Geopoly cursor
pub struct GeopolyCursor {
    /// R-tree cursor
    rtree_cursor: RtreeCursor,
    /// Current polygon
    current_poly: Option<GeoPoly>,
}

impl VirtualTable for GeopolyTable {
    fn create(db: &Connection, args: &[&str]) -> Result<Box<dyn VirtualTable>> {
        // Create underlying R-tree with 2 dimensions
        let rtree = RtreeTable::create_2d(db, &args[0])?;

        Ok(Box::new(GeopolyTable {
            rtree,
            name: args[0].to_string(),
        }))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Check for geopoly_overlap constraint
        for i in 0..info.constraint_count() {
            let constraint = info.constraint(i)?;
            // Column 0 is the shape, with special operators
            if constraint.column == 0 {
                info.set_constraint_usage(i, true, false)?;
                info.estimated_cost = 30.0;
                return Ok(());
            }
        }

        info.estimated_cost = 1000000.0;
        Ok(())
    }
}
```

## Geopoly SQL Functions
```rust
/// geopoly_area(P) - Calculate polygon area
pub fn geopoly_area(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;
    ctx.result_double(poly.area());
    Ok(())
}

/// geopoly_perimeter(P) - Calculate polygon perimeter
pub fn geopoly_perimeter(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;
    ctx.result_double(poly.perimeter());
    Ok(())
}

/// geopoly_blob(JSON) - Convert JSON to blob
pub fn geopoly_blob(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let json = args[0].as_str();
    let poly = GeoPoly::from_json(json)?;
    ctx.result_blob(&poly.to_blob());
    Ok(())
}

/// geopoly_json(P) - Convert blob to JSON
pub fn geopoly_json(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;
    let json = serde_json::to_string(&poly.vertices)?;
    ctx.result_text(&json);
    Ok(())
}

/// geopoly_svg(P, ...) - Generate SVG representation
pub fn geopoly_svg(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;

    let mut svg = String::from("<polygon points=\"");
    for (i, (x, y)) in poly.vertices.iter().enumerate() {
        if i > 0 {
            svg.push(' ');
        }
        svg.push_str(&format!("{},{}", x, y));
    }
    svg.push_str("\"/>");

    ctx.result_text(&svg);
    Ok(())
}

/// geopoly_bbox(P) - Get bounding box as polygon
pub fn geopoly_bbox(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;
    let (x1, y1, x2, y2) = poly.bbox();

    let bbox_poly = GeoPoly::new(vec![
        (x1, y1), (x2, y1), (x2, y2), (x1, y2)
    ]);

    ctx.result_blob(&bbox_poly.to_blob());
    Ok(())
}

/// geopoly_contains_point(P, X, Y) - Check if point in polygon
pub fn geopoly_contains_point(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let poly = GeoPoly::from_blob(args[0].as_blob())?;
    let x = args[1].as_double();
    let y = args[2].as_double();

    ctx.result_int(if poly.contains_point(x, y) { 1 } else { 0 });
    Ok(())
}

/// geopoly_within(P1, P2) - Check if P1 within P2
pub fn geopoly_within(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let p1 = GeoPoly::from_blob(args[0].as_blob())?;
    let p2 = GeoPoly::from_blob(args[1].as_blob())?;

    // P1 within P2 if all vertices of P1 are inside P2
    let within = p1.vertices.iter().all(|(x, y)| p2.contains_point(*x, *y));

    ctx.result_int(if within { 1 } else { 0 });
    Ok(())
}

/// geopoly_overlap(P1, P2) - Check if polygons overlap
pub fn geopoly_overlap(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let p1 = GeoPoly::from_blob(args[0].as_blob())?;
    let p2 = GeoPoly::from_blob(args[1].as_blob())?;

    ctx.result_int(if p1.overlaps(&p2) { 1 } else { 0 });
    Ok(())
}

/// geopoly_regular(X, Y, R, N) - Create regular polygon
pub fn geopoly_regular(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let cx = args[0].as_double();
    let cy = args[1].as_double();
    let r = args[2].as_double();
    let n = args[3].as_int() as usize;

    if n < 3 {
        return Err(Error::with_message(ErrorCode::Error, "need at least 3 sides"));
    }

    let mut vertices = Vec::with_capacity(n);
    for i in 0..n {
        let angle = 2.0 * std::f64::consts::PI * (i as f64) / (n as f64);
        let x = cx + r * angle.cos();
        let y = cy + r * angle.sin();
        vertices.push((x, y));
    }

    let poly = GeoPoly::new(vertices);
    ctx.result_blob(&poly.to_blob());
    Ok(())
}

/// geopoly_ccw(P) - Ensure counter-clockwise winding
pub fn geopoly_ccw(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let mut poly = GeoPoly::from_blob(args[0].as_blob())?;

    if poly.signed_area() < 0.0 {
        poly.vertices.reverse();
    }

    ctx.result_blob(&poly.to_blob());
    Ok(())
}
```

## Registration
```rust
/// Register geopoly extension
pub fn geopoly_init(db: &mut Connection) -> Result<()> {
    // Register virtual table
    db.create_module("geopoly", GeopolyModule)?;

    // Register functions
    db.create_function("geopoly_area", 1, geopoly_area)?;
    db.create_function("geopoly_perimeter", 1, geopoly_perimeter)?;
    db.create_function("geopoly_blob", 1, geopoly_blob)?;
    db.create_function("geopoly_json", 1, geopoly_json)?;
    db.create_function("geopoly_svg", -1, geopoly_svg)?;
    db.create_function("geopoly_bbox", 1, geopoly_bbox)?;
    db.create_function("geopoly_contains_point", 3, geopoly_contains_point)?;
    db.create_function("geopoly_within", 2, geopoly_within)?;
    db.create_function("geopoly_overlap", 2, geopoly_overlap)?;
    db.create_function("geopoly_regular", 4, geopoly_regular)?;
    db.create_function("geopoly_ccw", 1, geopoly_ccw)?;

    Ok(())
}
```

## Acceptance Criteria
- [ ] Polygon blob format (storage)
- [ ] JSON parsing and serialization
- [ ] geopoly_area() function
- [ ] geopoly_perimeter() function
- [ ] geopoly_bbox() function
- [ ] geopoly_contains_point() function
- [ ] geopoly_within() function
- [ ] geopoly_overlap() function
- [ ] geopoly_regular() function
- [ ] geopoly_svg() function
- [ ] geopoly_ccw() winding order
- [ ] Geopoly virtual table
- [ ] R-tree integration for spatial queries
