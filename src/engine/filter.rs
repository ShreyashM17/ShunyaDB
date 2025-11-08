use crate::storage::record::FieldValue;

#[derive(Debug, Clone)]
pub enum Filter {
    ById(u64),
    ByKeyValueEq(String, FieldValue),
    ByKeyValueOp(String, String, FieldValue),
}

impl Filter {
    /// Accepts patterns like "id=1", "age>20", "name=Shreyash"
    pub fn parse(s: &str) -> Option<Self> {
        // Try operators in longest-first order
        let ops = ["<=", ">=", "=", ">", "<"];
        for op in ops {
            if let Some((k, v)) = s.split_once(op) {
                let k = k.trim();
                let v = v.trim();
                if k == "id" && op == "=" {
                    if let Ok(id) = v.parse::<u64>() {
                        return Some(Filter::ById(id));
                    } else {
                        return None;
                    }
                }
                let field = FieldValue::from_str_infer(v);
                if op == "=" {
                    return Some(Filter::ByKeyValueEq(k.to_string(), field));
                } else {
                    return Some(Filter::ByKeyValueOp(k.to_string(), op.to_string(), field));
                }
            }
        }
        None
    }
}
