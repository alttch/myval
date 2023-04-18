#!/usr/bin/env python3

f = open('src/df_math.rs', 'w')

for i in (8, 16, 32, 64, 128):
    data = [(f'i{i}', f'Int{i}Array')]
    if i <= 64:
        data.append((f'u{i}', f'UInt{i}Array'))
    if i >= 32 and i <= 64:
        data += [(f'f{i}', f'Float{i}Array')]
    for (kind, arr) in data:
        for op in ['add', 'sub', 'mul', 'div']:
            s = f"""
pub fn {kind}_{op}(&mut self, name: &str, value: {kind}) -> Result<(), Error> {{
    if let Some(pos) = self.get_column_index(name) {{
        self.{kind}_{op}_at(pos, value)
    }} else {{
        Err(Error::NotFound(name.to_owned()))
    }}
}}
pub fn {kind}_{op}_at(&mut self, index: usize, value: {kind}) -> Result<(), Error> {{
  math_op!(self, index, {arr}, value, {kind}::{op})
}}
"""
            f.write(s)
