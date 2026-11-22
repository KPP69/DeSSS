use std::path::PathBuf;
/// Portable root: directory of the executable. All persistence lives in ./data next to the exe.
/// In dev (cargo run) current_exe is target/debug/desss(.exe) — data still sits next to that exe, never in OS config dirs.
#[derive(Clone, Debug)]
pub struct PortablePaths { pub root: PathBuf, pub data_dir: PathBuf, pub db_path: PathBuf, pub log_dir: PathBuf }
impl PortablePaths {
  pub fn resolve() -> Self {
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let root = exe.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));
    let data_dir = root.join("data"); let log_dir = data_dir.join("logs"); let db_path = data_dir.join("desss.db");
    Self{root,data_dir,db_path,log_dir}
  }
  pub fn ensure_dirs(&self) -> std::io::Result<()> { std::fs::create_dir_all(&self.data_dir)?; std::fs::create_dir_all(&self.log_dir)?; Ok(()) }
}
