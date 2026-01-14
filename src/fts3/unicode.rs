pub fn unicode_isalnum(c: i32) -> bool {
    if c < 0 {
        return false;
    }
    const ASCII: [u32; 4] = [0xFFFFFFFF, 0xFC00FFFF, 0xF8000001, 0xF8000001];
    const ENTRY: [u32; 406] = [
        0x00000030, 0x0000E807, 0x00016C06, 0x0001EC2F, 0x0002AC07, 0x0002D001, 0x0002D803,
        0x0002EC01, 0x0002FC01, 0x00035C01, 0x0003DC01, 0x000B0804, 0x000B480E, 0x000B9407,
        0x000BB401, 0x000BBC81, 0x000DD401, 0x000DF801, 0x000E1002, 0x000E1C01, 0x000FD801,
        0x00120808, 0x00156806, 0x00162402, 0x00163C01, 0x00164437, 0x0017CC02, 0x00180005,
        0x00181816, 0x00187802, 0x00192C15, 0x0019A804, 0x0019C001, 0x001B5001, 0x001B580F,
        0x001B9C07, 0x001BF402, 0x001C000E, 0x001C3C01, 0x001C4401, 0x001CC01B, 0x001E980B,
        0x001FAC09, 0x001FD804, 0x00205804, 0x00206C09, 0x00209403, 0x0020A405, 0x0020C00F,
        0x00216403, 0x00217801, 0x0023901B, 0x00240004, 0x0024E803, 0x0024F812, 0x00254407,
        0x00258804, 0x0025C001, 0x00260403, 0x0026F001, 0x0026F807, 0x00271C02, 0x00272C03,
        0x00275C01, 0x00278802, 0x0027C802, 0x0027E802, 0x00280403, 0x0028F001, 0x0028F805,
        0x00291C02, 0x00292C03, 0x00294401, 0x0029C002, 0x0029D401, 0x002A0403, 0x002AF001,
        0x002AF808, 0x002B1C03, 0x002B2C03, 0x002B8802, 0x002BC002, 0x002C0403, 0x002CF001,
        0x002CF807, 0x002D1C02, 0x002D2C03, 0x002D5802, 0x002D8802, 0x002DC001, 0x002E0801,
        0x002EF805, 0x002F1803, 0x002F2804, 0x002F5C01, 0x002FCC08, 0x00300403, 0x0030F807,
        0x00311803, 0x00312804, 0x00315402, 0x00318802, 0x0031FC01, 0x00320802, 0x0032F001,
        0x0032F807, 0x00331803, 0x00332804, 0x00335402, 0x00338802, 0x00340802, 0x0034F807,
        0x00351803, 0x00352804, 0x00355C01, 0x00358802, 0x0035E401, 0x00360802, 0x00372801,
        0x00373C06, 0x00375801, 0x00376008, 0x0037C803, 0x0038C401, 0x0038D007, 0x0038FC01,
        0x00391C09, 0x00396802, 0x003AC401, 0x003AD006, 0x003AEC02, 0x003B2006, 0x003C041F,
        0x003CD00C, 0x003DC417, 0x003E340B, 0x003E6424, 0x003EF80F, 0x003F380D, 0x0040AC14,
        0x00412806, 0x00415804, 0x00417803, 0x00418803, 0x00419C07, 0x0041C404, 0x0042080C,
        0x00423C01, 0x00426806, 0x0043EC01, 0x004D740C, 0x004E400A, 0x00500001, 0x0059B402,
        0x005A0001, 0x005A6C02, 0x005BAC03, 0x005C4803, 0x005CC805, 0x005D4802, 0x005DC802,
        0x005ED023, 0x005F6004, 0x005F7401, 0x0060000F, 0x0062A401, 0x0064800C, 0x0064C00C,
        0x00650001, 0x00651002, 0x0066C011, 0x00672002, 0x00677822, 0x00685C05, 0x00687802,
        0x0069540A, 0x0069801D, 0x0069FC01, 0x006A8007, 0x006AA006, 0x006C0005, 0x006CD011,
        0x006D6823, 0x006E0003, 0x006E840D, 0x006F980E, 0x006FF004, 0x00709014, 0x0070EC05,
        0x0071F802, 0x00730008, 0x00734019, 0x0073B401, 0x0073C803, 0x00770027, 0x0077F004,
        0x007EF401, 0x007EFC03, 0x007F3403, 0x007F7403, 0x007FB403, 0x007FF402, 0x00800065,
        0x0081A806, 0x0081E805, 0x00822805, 0x0082801A, 0x00834021, 0x00840002, 0x00840C04,
        0x00842002, 0x00845001, 0x00845803, 0x00847806, 0x00849401, 0x00849C01, 0x0084A401,
        0x0084B801, 0x0084E802, 0x00850005, 0x00852804, 0x00853C01, 0x00864264, 0x00900027,
        0x0091000B, 0x0092704E, 0x00940200, 0x009C0475, 0x009E53B9, 0x00AD400A, 0x00B39406,
        0x00B3BC03, 0x00B3E404, 0x00B3F802, 0x00B5C001, 0x00B5FC01, 0x00B7804F, 0x00B8C00C,
        0x00BA001A, 0x00BA6C59, 0x00BC00D6, 0x00BFC00C, 0x00C00005, 0x00C02019, 0x00C0A807,
        0x00C0D802, 0x00C0F403, 0x00C26404, 0x00C28001, 0x00C3EC01, 0x00C64002, 0x00C6580A,
        0x00C70024, 0x00C8001F, 0x00C8A81E, 0x00C94001, 0x00C98020, 0x00CA2827, 0x00CB003F,
        0x00CC0100, 0x01370040, 0x02924037, 0x0293F802, 0x02983403, 0x0299BC10, 0x029A7C01,
        0x029BC008, 0x029C0017, 0x029C8002, 0x029E2402, 0x02A00801, 0x02A01801, 0x02A02C01,
        0x02A08C09, 0x02A0D804, 0x02A1D004, 0x02A20002, 0x02A2D011, 0x02A33802, 0x02A38012,
        0x02A3E003, 0x02A4980A, 0x02A51C0D, 0x02A57C01, 0x02A60004, 0x02A6CC1B, 0x02A77802,
        0x02A8A40E, 0x02A90C01, 0x02A93002, 0x02A97004, 0x02A9DC03, 0x02A9EC01, 0x02AAC001,
        0x02AAC803, 0x02AADC02, 0x02AAF802, 0x02AB0401, 0x02AB7802, 0x02ABAC07, 0x02ABD402,
        0x02AF8C0B, 0x03600001, 0x036DFC02, 0x036FFC02, 0x037FFC01, 0x03EC7801, 0x03ECA401,
        0x03EEC810, 0x03F4F802, 0x03F7F002, 0x03F8001A, 0x03F88007, 0x03F8C023, 0x03F95013,
        0x03F9A004, 0x03FBFC01, 0x03FC040F, 0x03FC6807, 0x03FCEC06, 0x03FD6C0B, 0x03FF8007,
        0x03FFA007, 0x03FFE405, 0x04040003, 0x0404DC09, 0x0405E411, 0x0406400C, 0x0407402E,
        0x040E7C01, 0x040F4001, 0x04215C01, 0x04247C01, 0x0424FC01, 0x04280403, 0x04281402,
        0x04283004, 0x0428E003, 0x0428FC01, 0x04294009, 0x0429FC01, 0x042CE407, 0x04400003,
        0x0440E016, 0x04420003, 0x0442C012, 0x04440003, 0x04449C0E, 0x04450004, 0x04460003,
        0x0446CC0E, 0x04471404, 0x045AAC0D, 0x0491C004, 0x05BD442E, 0x05BE3C04, 0x074000F6,
        0x07440027, 0x0744A4B5, 0x07480046, 0x074C0057, 0x075B0401, 0x075B6C01, 0x075BEC01,
        0x075C5401, 0x075CD401, 0x075D3C01, 0x075DBC01, 0x075E2401, 0x075EA401, 0x075F0C01,
        0x07BBC002, 0x07C0002C, 0x07C0C064, 0x07C2800F, 0x07C2C40E, 0x07C3040F, 0x07C3440F,
        0x07C4401F, 0x07C4C03C, 0x07C5C02B, 0x07C7981D, 0x07C8402B, 0x07C90009, 0x07C94002,
        0x07CC0021, 0x07CCC006, 0x07CCDC46, 0x07CE0014, 0x07CE8025, 0x07CF1805, 0x07CF8011,
        0x07D0003F, 0x07D10001, 0x07D108B6, 0x07D3E404, 0x07D4003E, 0x07D50004, 0x07D54018,
        0x07D7EC46, 0x07D9140B, 0x07DA0046, 0x07DC0074, 0x38000401, 0x38008060, 0x380400F0,
    ];
    let c_u = c as u32;
    if c_u < 128 {
        return (ASCII[(c_u >> 5) as usize] & (1u32 << (c_u & 0x1f))) == 0;
    }
    if c_u < (1 << 22) {
        let key = (c_u << 10) | 0x000003ff;
        let mut i_lo = 0i32;
        let mut i_hi = (ENTRY.len() as i32) - 1;
        let mut i_res = 0i32;
        while i_hi >= i_lo {
            let i_test = (i_hi + i_lo) / 2;
            if key >= ENTRY[i_test as usize] {
                i_res = i_test;
                i_lo = i_test + 1;
            } else {
                i_hi = i_test - 1;
            }
        }
        let entry = ENTRY[i_res as usize];
        return c_u >= ((entry >> 10) + (entry & 0x3ff));
    }
    true
}

pub fn unicode_isdiacritic(c: i32) -> bool {
    if c < 768 || c > 817 {
        return false;
    }
    let mask0: u32 = 0x08029fdf;
    let mask1: u32 = 0x000361f8;
    if c < 768 + 32 {
        (mask0 & (1u32 << ((c - 768) as u32))) != 0
    } else {
        (mask1 & (1u32 << ((c - 768 - 32) as u32))) != 0
    }
}

fn remove_diacritic(c: i32, complex: bool) -> i32 {
    const A_DIA: [u16; 126] = [
        0, 1797, 1848, 1859, 1891, 1928, 1940, 1995, 2024, 2040, 2060, 2110, 2168, 2206, 2264,
        2286, 2344, 2383, 2472, 2488, 2516, 2596, 2668, 2732, 2782, 2842, 2894, 2954, 2984, 3000,
        3028, 3336, 3456, 3696, 3712, 3728, 3744, 3766, 3832, 3896, 3912, 3928, 3944, 3968, 4008,
        4040, 4056, 4106, 4138, 4170, 4202, 4234, 4266, 4296, 4312, 4344, 4408, 4424, 4442, 4472,
        4488, 4504, 6148, 6198, 6264, 6280, 6360, 6429, 6505, 6529, 61448, 61468, 61512, 61534,
        61592, 61610, 61642, 61672, 61688, 61704, 61726, 61784, 61800, 61816, 61836, 61880, 61896,
        61914, 61948, 61998, 62062, 62122, 62154, 62184, 62200, 62218, 62252, 62302, 62364, 62410,
        62442, 62478, 62536, 62554, 62584, 62604, 62640, 62648, 62656, 62664, 62730, 62766, 62830,
        62890, 62924, 62974, 63032, 63050, 63082, 63118, 63182, 63242, 63274, 63310, 63368, 63390,
    ];
    const A_CHAR: [u8; 126] = [
        0, 97, 99, 101, 105, 110, 111, 117, 121, 121, 97, 99, 100, 101, 101, 103, 104, 105, 106,
        107, 108, 110, 111, 114, 115, 116, 117, 117, 119, 121, 122, 111, 117, 97, 105, 111, 117,
        245, 225, 103, 107, 111, 239, 106, 103, 110, 225, 97, 101, 105, 111, 114, 117, 115, 116,
        104, 97, 101, 239, 111, 239, 121, 0, 0, 0, 0, 0, 0, 0, 0, 97, 98, 227, 100, 100, 229, 101,
        229, 102, 103, 104, 104, 105, 233, 107, 108, 236, 108, 109, 110, 239, 112, 114, 242, 114,
        115, 243, 116, 117, 245, 118, 119, 119, 120, 121, 122, 104, 116, 119, 121, 97, 225, 225,
        225, 101, 229, 229, 105, 111, 239, 239, 239, 117, 245, 245, 121,
    ];
    let mut i_lo: i32 = 0;
    let mut i_hi: i32 = (A_DIA.len() as i32) - 1;
    let mut i_res: i32 = -1;
    while i_hi >= i_lo {
        let i_test = (i_hi + i_lo) / 2;
        let entry = A_DIA[i_test as usize] as i32;
        let code = entry >> 3;
        if c >= code {
            i_res = i_test;
            i_lo = i_test + 1;
        } else {
            i_hi = i_test - 1;
        }
    }
    if i_res < 0 {
        return c;
    }
    let entry = A_DIA[i_res as usize] as i32;
    let base = entry >> 3;
    let n = entry & 0x7;
    let ch = A_CHAR[i_res as usize];
    if !complex && (ch & 0x80) != 0 {
        return c;
    }
    if c > base + n {
        c
    } else {
        (ch & 0x7f) as i32
    }
}

pub fn unicode_fold(c: i32, remove_diacritics: i32) -> i32 {
    if c < 0 {
        return c;
    }
    #[derive(Clone, Copy)]
    struct FoldEntry {
        i_code: u16,
        flags: u8,
        n_range: u8,
    }
    const ENTRY: [FoldEntry; 163] = [
        FoldEntry {
            i_code: 65,
            flags: 14,
            n_range: 26,
        },
        FoldEntry {
            i_code: 181,
            flags: 64,
            n_range: 1,
        },
        FoldEntry {
            i_code: 192,
            flags: 14,
            n_range: 23,
        },
        FoldEntry {
            i_code: 216,
            flags: 14,
            n_range: 7,
        },
        FoldEntry {
            i_code: 256,
            flags: 1,
            n_range: 48,
        },
        FoldEntry {
            i_code: 306,
            flags: 1,
            n_range: 6,
        },
        FoldEntry {
            i_code: 313,
            flags: 1,
            n_range: 16,
        },
        FoldEntry {
            i_code: 330,
            flags: 1,
            n_range: 46,
        },
        FoldEntry {
            i_code: 376,
            flags: 116,
            n_range: 1,
        },
        FoldEntry {
            i_code: 377,
            flags: 1,
            n_range: 6,
        },
        FoldEntry {
            i_code: 383,
            flags: 104,
            n_range: 1,
        },
        FoldEntry {
            i_code: 385,
            flags: 50,
            n_range: 1,
        },
        FoldEntry {
            i_code: 386,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 390,
            flags: 44,
            n_range: 1,
        },
        FoldEntry {
            i_code: 391,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 393,
            flags: 42,
            n_range: 2,
        },
        FoldEntry {
            i_code: 395,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 398,
            flags: 32,
            n_range: 1,
        },
        FoldEntry {
            i_code: 399,
            flags: 38,
            n_range: 1,
        },
        FoldEntry {
            i_code: 400,
            flags: 40,
            n_range: 1,
        },
        FoldEntry {
            i_code: 401,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 403,
            flags: 42,
            n_range: 1,
        },
        FoldEntry {
            i_code: 404,
            flags: 46,
            n_range: 1,
        },
        FoldEntry {
            i_code: 406,
            flags: 52,
            n_range: 1,
        },
        FoldEntry {
            i_code: 407,
            flags: 48,
            n_range: 1,
        },
        FoldEntry {
            i_code: 408,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 412,
            flags: 52,
            n_range: 1,
        },
        FoldEntry {
            i_code: 413,
            flags: 54,
            n_range: 1,
        },
        FoldEntry {
            i_code: 415,
            flags: 56,
            n_range: 1,
        },
        FoldEntry {
            i_code: 416,
            flags: 1,
            n_range: 6,
        },
        FoldEntry {
            i_code: 422,
            flags: 60,
            n_range: 1,
        },
        FoldEntry {
            i_code: 423,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 425,
            flags: 60,
            n_range: 1,
        },
        FoldEntry {
            i_code: 428,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 430,
            flags: 60,
            n_range: 1,
        },
        FoldEntry {
            i_code: 431,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 433,
            flags: 58,
            n_range: 2,
        },
        FoldEntry {
            i_code: 435,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 439,
            flags: 62,
            n_range: 1,
        },
        FoldEntry {
            i_code: 440,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 444,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 452,
            flags: 2,
            n_range: 1,
        },
        FoldEntry {
            i_code: 453,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 455,
            flags: 2,
            n_range: 1,
        },
        FoldEntry {
            i_code: 456,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 458,
            flags: 2,
            n_range: 1,
        },
        FoldEntry {
            i_code: 459,
            flags: 1,
            n_range: 18,
        },
        FoldEntry {
            i_code: 478,
            flags: 1,
            n_range: 18,
        },
        FoldEntry {
            i_code: 497,
            flags: 2,
            n_range: 1,
        },
        FoldEntry {
            i_code: 498,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 502,
            flags: 122,
            n_range: 1,
        },
        FoldEntry {
            i_code: 503,
            flags: 134,
            n_range: 1,
        },
        FoldEntry {
            i_code: 504,
            flags: 1,
            n_range: 40,
        },
        FoldEntry {
            i_code: 544,
            flags: 110,
            n_range: 1,
        },
        FoldEntry {
            i_code: 546,
            flags: 1,
            n_range: 18,
        },
        FoldEntry {
            i_code: 570,
            flags: 70,
            n_range: 1,
        },
        FoldEntry {
            i_code: 571,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 573,
            flags: 108,
            n_range: 1,
        },
        FoldEntry {
            i_code: 574,
            flags: 68,
            n_range: 1,
        },
        FoldEntry {
            i_code: 577,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 579,
            flags: 106,
            n_range: 1,
        },
        FoldEntry {
            i_code: 580,
            flags: 28,
            n_range: 1,
        },
        FoldEntry {
            i_code: 581,
            flags: 30,
            n_range: 1,
        },
        FoldEntry {
            i_code: 582,
            flags: 1,
            n_range: 10,
        },
        FoldEntry {
            i_code: 837,
            flags: 36,
            n_range: 1,
        },
        FoldEntry {
            i_code: 880,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 886,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 902,
            flags: 18,
            n_range: 1,
        },
        FoldEntry {
            i_code: 904,
            flags: 16,
            n_range: 3,
        },
        FoldEntry {
            i_code: 908,
            flags: 26,
            n_range: 1,
        },
        FoldEntry {
            i_code: 910,
            flags: 24,
            n_range: 2,
        },
        FoldEntry {
            i_code: 913,
            flags: 14,
            n_range: 17,
        },
        FoldEntry {
            i_code: 931,
            flags: 14,
            n_range: 9,
        },
        FoldEntry {
            i_code: 962,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 975,
            flags: 4,
            n_range: 1,
        },
        FoldEntry {
            i_code: 976,
            flags: 140,
            n_range: 1,
        },
        FoldEntry {
            i_code: 977,
            flags: 142,
            n_range: 1,
        },
        FoldEntry {
            i_code: 981,
            flags: 146,
            n_range: 1,
        },
        FoldEntry {
            i_code: 982,
            flags: 144,
            n_range: 1,
        },
        FoldEntry {
            i_code: 984,
            flags: 1,
            n_range: 24,
        },
        FoldEntry {
            i_code: 1008,
            flags: 136,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1009,
            flags: 138,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1012,
            flags: 130,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1013,
            flags: 128,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1015,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1017,
            flags: 152,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1018,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1021,
            flags: 110,
            n_range: 3,
        },
        FoldEntry {
            i_code: 1024,
            flags: 34,
            n_range: 16,
        },
        FoldEntry {
            i_code: 1040,
            flags: 14,
            n_range: 32,
        },
        FoldEntry {
            i_code: 1120,
            flags: 1,
            n_range: 34,
        },
        FoldEntry {
            i_code: 1162,
            flags: 1,
            n_range: 54,
        },
        FoldEntry {
            i_code: 1216,
            flags: 6,
            n_range: 1,
        },
        FoldEntry {
            i_code: 1217,
            flags: 1,
            n_range: 14,
        },
        FoldEntry {
            i_code: 1232,
            flags: 1,
            n_range: 88,
        },
        FoldEntry {
            i_code: 1329,
            flags: 22,
            n_range: 38,
        },
        FoldEntry {
            i_code: 4256,
            flags: 66,
            n_range: 38,
        },
        FoldEntry {
            i_code: 4295,
            flags: 66,
            n_range: 1,
        },
        FoldEntry {
            i_code: 4301,
            flags: 66,
            n_range: 1,
        },
        FoldEntry {
            i_code: 7680,
            flags: 1,
            n_range: 150,
        },
        FoldEntry {
            i_code: 7835,
            flags: 132,
            n_range: 1,
        },
        FoldEntry {
            i_code: 7838,
            flags: 96,
            n_range: 1,
        },
        FoldEntry {
            i_code: 7840,
            flags: 1,
            n_range: 96,
        },
        FoldEntry {
            i_code: 7944,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 7960,
            flags: 150,
            n_range: 6,
        },
        FoldEntry {
            i_code: 7976,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 7992,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8008,
            flags: 150,
            n_range: 6,
        },
        FoldEntry {
            i_code: 8025,
            flags: 151,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8040,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8072,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8088,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8104,
            flags: 150,
            n_range: 8,
        },
        FoldEntry {
            i_code: 8120,
            flags: 150,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8122,
            flags: 126,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8124,
            flags: 148,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8126,
            flags: 100,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8136,
            flags: 124,
            n_range: 4,
        },
        FoldEntry {
            i_code: 8140,
            flags: 148,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8152,
            flags: 150,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8154,
            flags: 120,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8168,
            flags: 150,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8170,
            flags: 118,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8172,
            flags: 152,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8184,
            flags: 112,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8186,
            flags: 114,
            n_range: 2,
        },
        FoldEntry {
            i_code: 8188,
            flags: 148,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8486,
            flags: 98,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8490,
            flags: 92,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8491,
            flags: 94,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8498,
            flags: 12,
            n_range: 1,
        },
        FoldEntry {
            i_code: 8544,
            flags: 8,
            n_range: 16,
        },
        FoldEntry {
            i_code: 8579,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 9398,
            flags: 10,
            n_range: 26,
        },
        FoldEntry {
            i_code: 11264,
            flags: 22,
            n_range: 47,
        },
        FoldEntry {
            i_code: 11360,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11362,
            flags: 88,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11363,
            flags: 102,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11364,
            flags: 90,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11367,
            flags: 1,
            n_range: 6,
        },
        FoldEntry {
            i_code: 11373,
            flags: 84,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11374,
            flags: 86,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11375,
            flags: 80,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11376,
            flags: 82,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11378,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11381,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 11390,
            flags: 78,
            n_range: 2,
        },
        FoldEntry {
            i_code: 11392,
            flags: 1,
            n_range: 100,
        },
        FoldEntry {
            i_code: 11499,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 11506,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 42560,
            flags: 1,
            n_range: 46,
        },
        FoldEntry {
            i_code: 42624,
            flags: 1,
            n_range: 24,
        },
        FoldEntry {
            i_code: 42786,
            flags: 1,
            n_range: 14,
        },
        FoldEntry {
            i_code: 42802,
            flags: 1,
            n_range: 62,
        },
        FoldEntry {
            i_code: 42873,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 42877,
            flags: 76,
            n_range: 1,
        },
        FoldEntry {
            i_code: 42878,
            flags: 1,
            n_range: 10,
        },
        FoldEntry {
            i_code: 42891,
            flags: 0,
            n_range: 1,
        },
        FoldEntry {
            i_code: 42893,
            flags: 74,
            n_range: 1,
        },
        FoldEntry {
            i_code: 42896,
            flags: 1,
            n_range: 4,
        },
        FoldEntry {
            i_code: 42912,
            flags: 1,
            n_range: 10,
        },
        FoldEntry {
            i_code: 42922,
            flags: 72,
            n_range: 1,
        },
        FoldEntry {
            i_code: 65313,
            flags: 14,
            n_range: 26,
        },
    ];
    const AI_OFF: [u16; 77] = [
        1, 2, 8, 15, 16, 26, 28, 32, 37, 38, 40, 48, 63, 64, 69, 71, 79, 80, 116, 202, 203, 205,
        206, 207, 209, 210, 211, 213, 214, 217, 218, 219, 775, 7264, 10792, 10795, 23228, 23256,
        30204, 54721, 54753, 54754, 54756, 54787, 54793, 54809, 57153, 57274, 57921, 58019, 58363,
        61722, 65268, 65341, 65373, 65406, 65408, 65410, 65415, 65424, 65436, 65439, 65450, 65462,
        65472, 65476, 65478, 65480, 65482, 65488, 65506, 65511, 65514, 65521, 65527, 65528, 65529,
    ];
    let mut ret = c;
    if c < 128 {
        if c >= (b"A"[0] as i32) && c <= (b"Z"[0] as i32) {
            ret = c + ((b"a"[0] as i32) - (b"A"[0] as i32));
        }
    } else if c < 65536 {
        let mut i_lo: i32 = 0;
        let mut i_hi: i32 = (ENTRY.len() as i32) - 1;
        let mut i_res: i32 = -1;
        while i_hi >= i_lo {
            let i_test = (i_hi + i_lo) / 2;
            let cmp = c - ENTRY[i_test as usize].i_code as i32;
            if cmp >= 0 {
                i_res = i_test;
                i_lo = i_test + 1;
            } else {
                i_hi = i_test - 1;
            }
        }
        if i_res >= 0 {
            let entry = ENTRY[i_res as usize];
            let code = entry.i_code as i32;
            if c < code + entry.n_range as i32 {
                let flag = entry.flags;
                if (flag & 0x01) == 0 || ((code ^ c) & 0x01) == 0 {
                    ret = ((c + AI_OFF[(flag >> 1) as usize] as i32) & 0x0000ffff) as i32;
                }
            }
            if remove_diacritics != 0 {
                ret = remove_diacritic(ret, remove_diacritics == 2);
            }
        }
    } else if c >= 66560 && c < 66600 {
        ret = c + 40;
    }
    ret
}
