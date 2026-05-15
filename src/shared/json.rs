//! serde_json::Value 操作工具。响应清洗会反复删字段、改路径，
//! 把模式固化在这里避免每个 channel 各写一遍。

use serde_json::Value;

/// 按路径删除一个键，返回是否真的删了。
/// 空 path 直接返回 false（顶层用 strip_keys）。中间节点必须是 Object，否则放弃。
pub fn delete_path(obj: &mut Value, path: &[&str]) -> bool {
    if path.is_empty() {
        return false;
    }
    let (last, parents) = path.split_last().expect("path non-empty");
    let mut cursor = obj;
    for key in parents {
        match cursor {
            Value::Object(map) => match map.get_mut(*key) {
                Some(next) => cursor = next,
                None => return false,
            },
            _ => return false,
        }
    }
    match cursor {
        Value::Object(map) => map.remove(*last).is_some(),
        _ => false,
    }
}

/// 顶层批量删除。返回实际删除数量。
pub fn strip_keys(obj: &mut Value, keys: &[&str]) -> u32 {
    let Value::Object(map) = obj else {
        return 0;
    };
    let mut n = 0;
    for k in keys {
        if map.remove(*k).is_some() {
            n += 1;
        }
    }
    n
}

/// 取 Object 的可变借用，非 Object 返 None。
pub fn as_object_mut(v: &mut Value) -> Option<&mut serde_json::Map<String, Value>> {
    match v {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

/// 取 Array 的可变借用，非 Array 返 None。
pub fn as_array_mut(v: &mut Value) -> Option<&mut Vec<Value>> {
    match v {
        Value::Array(a) => Some(a),
        _ => None,
    }
}

/// 判一个 Value 是不是空字符串（用于过滤空 text block 等）。
pub fn is_empty_string(v: &Value) -> bool {
    matches!(v, Value::String(s) if s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strip_keys_basic() {
        let mut v = json!({ "a": 1, "b": 2, "c": 3 });
        let n = strip_keys(&mut v, &["a", "c", "missing"]);
        assert_eq!(n, 2);
        assert_eq!(v, json!({ "b": 2 }));
    }

    #[test]
    fn strip_keys_non_object_noop() {
        let mut v = json!([1, 2, 3]);
        assert_eq!(strip_keys(&mut v, &["a"]), 0);
    }

    #[test]
    fn delete_path_nested() {
        let mut v = json!({ "outer": { "inner": { "leaf": 42 }, "keep": true } });
        assert!(delete_path(&mut v, &["outer", "inner", "leaf"]));
        assert_eq!(v, json!({ "outer": { "inner": {}, "keep": true } }));
    }

    #[test]
    fn delete_path_missing_returns_false() {
        let mut v = json!({ "a": { "b": 1 } });
        assert!(!delete_path(&mut v, &["a", "missing"]));
        assert!(!delete_path(&mut v, &["nope", "b"]));
        assert!(!delete_path(&mut v, &[]));
    }

    #[test]
    fn delete_path_through_array_fails() {
        let mut v = json!({ "arr": [1, 2, 3] });
        assert!(!delete_path(&mut v, &["arr", "0"]));
    }

    #[test]
    fn as_object_mut_works() {
        let mut v = json!({ "a": 1 });
        let m = as_object_mut(&mut v).expect("is object");
        m.insert("b".into(), json!(2));
        assert_eq!(v, json!({ "a": 1, "b": 2 }));
    }

    #[test]
    fn is_empty_string_detector() {
        assert!(is_empty_string(&json!("")));
        assert!(!is_empty_string(&json!("x")));
        assert!(!is_empty_string(&json!(0)));
        assert!(!is_empty_string(&json!(null)));
    }
}
