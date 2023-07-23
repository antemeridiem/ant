use std::collections::HashMap;

//
//
//

#[derive(Debug, serde::Serialize)]
pub enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

#[derive(Debug, serde::Serialize)]
pub struct Diff {
    pub new: HashMap<String, Vec<String>>,
    pub old: HashMap<String, Vec<String>>,
    pub diff: HashMap<String, [Vec<String>; 2]>,
}
impl Diff {
    pub fn new() -> Self {
        Self {
            new: HashMap::new(),
            old: HashMap::new(),
            diff: HashMap::new(),
        }
    }
}

pub fn run_json(left: &serde_json::Value, right: &serde_json::Value, path: &str, diff: &mut Diff) -> Result<(), Box<dyn std::error::Error>> {
    match left {
        left if left.is_object() & right.is_object() => {
            let keys_left = &map_keys_get(left)?;
            let keys_right = &map_keys_get(right)?;

            let keys_new = keys_left
                .iter()
                .filter(|x| !keys_right.contains(x))
                .map(|x| x.to_string().clone())
                .collect::<Vec<String>>();
            let keys_old = keys_right
                .iter()
                .filter(|x| !keys_left.contains(x))
                .map(|x| x.to_string().clone())
                .collect::<Vec<String>>();
            let keys_common = keys_left
                .iter()
                .filter(|x| keys_right.contains(x))
                .map(|x| *x)
                .collect::<Vec<&String>>();

            if !keys_new.is_empty() {
                diff.new.insert(path.to_string(), keys_new);
            }
            if !keys_old.is_empty() {
                diff.old.insert(path.to_string(), keys_old);
            }

            for key in keys_common.iter() {
                run_json(&left[key], &right[key], &format!("{}/{}", path, key), diff)?
            }
        }
        left if left.is_array() & right.is_array() => {
            let arr_left = arr_elements_get(left)?;
            let arr_right = arr_elements_get(right)?;

            let elements_new = arr_left
                .iter()
                .filter(|x| !arr_right.contains(x))
                .map(|x| Ok(serde_json::to_string(x)?))
                .collect::<Result<Vec<String>, Box<dyn std::error::Error>>>()?;
            let elements_old = arr_right
                .iter()
                .filter(|x| !arr_left.contains(x))
                .map(|x| Ok(serde_json::to_string(x)?))
                .collect::<Result<Vec<String>, Box<dyn std::error::Error>>>()?;

            if !elements_new.is_empty() | !elements_old.is_empty() {
                diff.diff.insert(path.to_string(), [elements_new, elements_old]);
            }
        }
        left => {
            if left != right {
                diff.diff.insert(
                    path.to_string(),
                    [
                        Vec::from([serde_json::to_string(right)?]),
                        Vec::from([serde_json::to_string(left)?]),
                    ],
                );
            }
        }
    }

    Ok({})
}

fn map_keys_get(map: &serde_json::Value) -> Result<Vec<&String>, Box<dyn std::error::Error>> {
    Ok(map.as_object()
        .ok_or("map not found")?
        .keys()
        .collect::<Vec<&String>>())
}

fn arr_elements_get(arr: &serde_json::Value) -> Result<&Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Ok(arr.as_array().ok_or("arr not found")?)
}


