const CTYPE: [u8; 26] = [
    0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 2, 1,
];

fn is_consonant(z: &[u8]) -> bool {
    let x = *z.get(0).unwrap_or(&0);
    if x == 0 {
        return false;
    }
    let j = CTYPE[(x - b'a') as usize];
    if j < 2 {
        return j == 1;
    }
    z.get(1).copied().unwrap_or(0) == 0 || is_vowel(&z[1..])
}

fn is_vowel(z: &[u8]) -> bool {
    let x = *z.get(0).unwrap_or(&0);
    if x == 0 {
        return false;
    }
    let j = CTYPE[(x - b'a') as usize];
    if j < 2 {
        return j == 0;
    }
    is_consonant(&z[1..])
}

fn m_gt_0(z: &[u8]) -> bool {
    let mut idx = 0;
    while idx < z.len() && is_vowel(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_consonant(&z[idx..]) {
        idx += 1;
    }
    idx < z.len() && z[idx] != 0
}

fn m_eq_1(z: &[u8]) -> bool {
    let mut idx = 0;
    while idx < z.len() && is_vowel(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_consonant(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_vowel(&z[idx..]) {
        idx += 1;
    }
    idx >= z.len() || z[idx] == 0
}

fn m_gt_1(z: &[u8]) -> bool {
    let mut idx = 0;
    while idx < z.len() && is_vowel(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_consonant(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_vowel(&z[idx..]) {
        idx += 1;
    }
    if idx >= z.len() || z[idx] == 0 {
        return false;
    }
    while idx < z.len() && is_consonant(&z[idx..]) {
        idx += 1;
    }
    idx < z.len() && z[idx] != 0
}

fn has_vowel(z: &[u8]) -> bool {
    let mut idx = 0;
    while idx < z.len() && z[idx] != 0 {
        if is_vowel(&z[idx..]) {
            return true;
        }
        idx += 1;
    }
    false
}

fn double_consonant(z: &[u8]) -> bool {
    z.get(0).copied().unwrap_or(0) != 0
        && z.get(1).copied().unwrap_or(0) != 0
        && z[0] == z[1]
        && is_consonant(z)
}

fn star_oh(z: &[u8]) -> bool {
    is_consonant(z)
        && z[0] != b'w'
        && z[0] != b'x'
        && z[0] != b'y'
        && is_vowel(&z[1..])
        && is_consonant(&z[2..])
}

fn stem(
    buf: &mut [u8],
    start: &mut usize,
    from: &[u8],
    to: &[u8],
    cond: Option<fn(&[u8]) -> bool>,
) -> bool {
    let mut i = 0usize;
    while i < from.len() && buf[*start + i] == from[i] {
        i += 1;
    }
    if i < from.len() {
        return false;
    }
    if let Some(cond_fn) = cond {
        if !cond_fn(&buf[*start + i..]) {
            return true;
        }
    }
    for &ch in to {
        *start -= 1;
        buf[*start] = ch;
    }
    true
}

fn copy_stemmer(input: &[u8]) -> Vec<u8> {
    let mut has_digit = false;
    let mut out: Vec<u8> = Vec::with_capacity(input.len());
    for &c in input {
        if (b'A'..=b'Z').contains(&c) {
            out.push(c + (b'a' - b'A'));
        } else {
            if (b'0'..=b'9').contains(&c) {
                has_digit = true;
            }
            out.push(c);
        }
    }
    let mx = if has_digit { 3 } else { 10 };
    if out.len() > mx * 2 {
        let mut truncated = Vec::with_capacity(mx * 2);
        truncated.extend_from_slice(&out[..mx]);
        truncated.extend_from_slice(&out[out.len() - mx..]);
        return truncated;
    }
    out
}

pub fn porter_stem(input: &str) -> String {
    let bytes = input.as_bytes();
    let n_in = bytes.len();
    if n_in < 3 || n_in >= 21 {
        return String::from_utf8_lossy(&copy_stemmer(bytes)).to_string();
    }

    let mut z_reverse = [0u8; 28];
    let mut j = z_reverse.len() - 6;
    for &c in bytes {
        let lower = if (b'A'..=b'Z').contains(&c) {
            c + (b'a' - b'A')
        } else if (b'a'..=b'z').contains(&c) {
            c
        } else {
            return String::from_utf8_lossy(&copy_stemmer(bytes)).to_string();
        };
        z_reverse[j] = lower;
        j -= 1;
    }
    let start = j + 1;
    let mut z_start = start;

    let z = &mut z_reverse;

    if z[z_start] == b's' {
        if !stem(z, &mut z_start, b"sess", b"ss", None)
            && !stem(z, &mut z_start, b"sei", b"i", None)
            && !stem(z, &mut z_start, b"ss", b"ss", None)
        {
            z_start += 1;
        }
    }

    let z2 = z_start;
    if stem(z, &mut z_start, b"dee", b"ee", Some(m_gt_0)) {
        // no-op
    } else if (stem(z, &mut z_start, b"gni", b"", Some(has_vowel))
        || stem(z, &mut z_start, b"de", b"", Some(has_vowel)))
        && z_start != z2
    {
        if stem(z, &mut z_start, b"ta", b"ate", None)
            || stem(z, &mut z_start, b"lb", b"ble", None)
            || stem(z, &mut z_start, b"zi", b"ize", None)
        {
            // no-op
        } else if double_consonant(&z[z_start..])
            && z[z_start] != b'l'
            && z[z_start] != b's'
            && z[z_start] != b'z'
        {
            z_start += 1;
        } else if m_eq_1(&z[z_start..]) && star_oh(&z[z_start..]) {
            z_start -= 1;
            z[z_start] = b'e';
        }
    }

    if z[z_start] == b'y' && has_vowel(&z[z_start + 1..]) {
        z[z_start] = b'i';
    }

    match z.get(z_start + 1).copied().unwrap_or(0) {
        b'a' => {
            if !stem(z, &mut z_start, b"lanoita", b"ate", Some(m_gt_0)) {
                stem(z, &mut z_start, b"lanoit", b"tion", Some(m_gt_0));
            }
        }
        b'c' => {
            if !stem(z, &mut z_start, b"icne", b"ence", Some(m_gt_0)) {
                stem(z, &mut z_start, b"icna", b"ance", Some(m_gt_0));
            }
        }
        b'e' => {
            stem(z, &mut z_start, b"rezi", b"ize", Some(m_gt_0));
        }
        b'g' => {
            stem(z, &mut z_start, b"igol", b"log", Some(m_gt_0));
        }
        b'l' => {
            if !stem(z, &mut z_start, b"ilb", b"ble", Some(m_gt_0))
                && !stem(z, &mut z_start, b"illa", b"al", Some(m_gt_0))
                && !stem(z, &mut z_start, b"iltne", b"ent", Some(m_gt_0))
                && !stem(z, &mut z_start, b"ile", b"e", Some(m_gt_0))
            {
                stem(z, &mut z_start, b"ilsuo", b"ous", Some(m_gt_0));
            }
        }
        b'o' => {
            if !stem(z, &mut z_start, b"noitazi", b"ize", Some(m_gt_0))
                && !stem(z, &mut z_start, b"noita", b"ate", Some(m_gt_0))
            {
                stem(z, &mut z_start, b"rota", b"ate", Some(m_gt_0));
            }
        }
        b's' => {
            if !stem(z, &mut z_start, b"msila", b"al", Some(m_gt_0))
                && !stem(z, &mut z_start, b"ssenevi", b"ive", Some(m_gt_0))
                && !stem(z, &mut z_start, b"ssenluf", b"ful", Some(m_gt_0))
            {
                stem(z, &mut z_start, b"ssensuo", b"ous", Some(m_gt_0));
            }
        }
        b't' => {
            if !stem(z, &mut z_start, b"itila", b"al", Some(m_gt_0))
                && !stem(z, &mut z_start, b"itivi", b"ive", Some(m_gt_0))
            {
                stem(z, &mut z_start, b"itilib", b"ble", Some(m_gt_0));
            }
        }
        _ => {}
    }

    match z.get(z_start).copied().unwrap_or(0) {
        b'e' => {
            if !stem(z, &mut z_start, b"etaci", b"ic", Some(m_gt_0))
                && !stem(z, &mut z_start, b"evita", b"", Some(m_gt_0))
            {
                stem(z, &mut z_start, b"ezila", b"al", Some(m_gt_0));
            }
        }
        b'i' => {
            stem(z, &mut z_start, b"itici", b"ic", Some(m_gt_0));
        }
        b'l' => {
            if !stem(z, &mut z_start, b"laci", b"ic", Some(m_gt_0)) {
                stem(z, &mut z_start, b"luf", b"", Some(m_gt_0));
            }
        }
        b's' => {
            stem(z, &mut z_start, b"ssen", b"", Some(m_gt_0));
        }
        _ => {}
    }

    match z.get(z_start + 1).copied().unwrap_or(0) {
        b'a' => {
            if z.get(z_start).copied().unwrap_or(0) == b'l' && m_gt_1(&z[z_start + 2..]) {
                z_start += 2;
            }
        }
        b'c' => {
            if z.get(z_start).copied().unwrap_or(0) == b'e'
                && z.get(z_start + 2).copied().unwrap_or(0) == b'n'
                && (z.get(z_start + 3).copied().unwrap_or(0) == b'a'
                    || z.get(z_start + 3).copied().unwrap_or(0) == b'e')
                && m_gt_1(&z[z_start + 4..])
            {
                z_start += 4;
            }
        }
        b'e' => {
            if z.get(z_start).copied().unwrap_or(0) == b'r' && m_gt_1(&z[z_start + 2..]) {
                z_start += 2;
            }
        }
        b'i' => {
            if z.get(z_start).copied().unwrap_or(0) == b'c' && m_gt_1(&z[z_start + 2..]) {
                z_start += 2;
            }
        }
        b'l' => {
            if z.get(z_start).copied().unwrap_or(0) == b'e'
                && z.get(z_start + 2).copied().unwrap_or(0) == b'b'
                && (z.get(z_start + 3).copied().unwrap_or(0) == b'a'
                    || z.get(z_start + 3).copied().unwrap_or(0) == b'i')
                && m_gt_1(&z[z_start + 4..])
            {
                z_start += 4;
            }
        }
        b'n' => {
            if z.get(z_start).copied().unwrap_or(0) == b't' {
                if z.get(z_start + 2).copied().unwrap_or(0) == b'a' {
                    if m_gt_1(&z[z_start + 3..]) {
                        z_start += 3;
                    }
                } else if z.get(z_start + 2).copied().unwrap_or(0) == b'e' {
                    if !stem(z, &mut z_start, b"tneme", b"", Some(m_gt_1))
                        && !stem(z, &mut z_start, b"tnem", b"", Some(m_gt_1))
                    {
                        stem(z, &mut z_start, b"tne", b"", Some(m_gt_1));
                    }
                }
            }
        }
        b'o' => {
            if z.get(z_start).copied().unwrap_or(0) == b'u' {
                if m_gt_1(&z[z_start + 2..]) {
                    z_start += 2;
                }
            } else if z.get(z_start + 3).copied().unwrap_or(0) == b's'
                || z.get(z_start + 3).copied().unwrap_or(0) == b't'
            {
                stem(z, &mut z_start, b"noi", b"", Some(m_gt_1));
            }
        }
        b's' => {
            if z.get(z_start).copied().unwrap_or(0) == b'm'
                && z.get(z_start + 2).copied().unwrap_or(0) == b'i'
                && m_gt_1(&z[z_start + 3..])
            {
                z_start += 3;
            }
        }
        b't' => {
            if !stem(z, &mut z_start, b"eta", b"", Some(m_gt_1)) {
                stem(z, &mut z_start, b"iti", b"", Some(m_gt_1));
            }
        }
        b'u' => {
            if z.get(z_start).copied().unwrap_or(0) == b's'
                && z.get(z_start + 2).copied().unwrap_or(0) == b'o'
                && m_gt_1(&z[z_start + 3..])
            {
                z_start += 3;
            }
        }
        b'v' | b'z' => {
            if z.get(z_start).copied().unwrap_or(0) == b'e'
                && z.get(z_start + 2).copied().unwrap_or(0) == b'i'
                && m_gt_1(&z[z_start + 3..])
            {
                z_start += 3;
            }
        }
        _ => {}
    }

    if z.get(z_start).copied().unwrap_or(0) == b'e' {
        if m_gt_1(&z[z_start + 1..]) {
            z_start += 1;
        } else if m_eq_1(&z[z_start + 1..]) && !star_oh(&z[z_start + 1..]) {
            z_start += 1;
        }
    }

    if m_gt_1(&z[z_start..])
        && z.get(z_start).copied().unwrap_or(0) == b'l'
        && z.get(z_start + 1).copied().unwrap_or(0) == b'l'
    {
        z_start += 1;
    }

    let slice = &z[z_start..];
    let len = slice.iter().position(|&c| c == 0).unwrap_or(slice.len());
    let mut out = vec![0u8; len];
    let mut i = len;
    for &c in slice.iter().take(len) {
        i -= 1;
        out[i] = c;
    }
    String::from_utf8_lossy(&out).to_string()
}
