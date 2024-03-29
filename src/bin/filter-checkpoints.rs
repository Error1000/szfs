use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
};
use szfs::{
    dmu::{DNodeDirectoryContents, DNodePlainFileContents, ObjSet},
    zio::Vdevs,
    *,
};

#[derive(Debug, Serialize, Deserialize)]
struct IndirectBlock {
    pub bps: Vec<Option<zio::BlockPointer>>,
}

#[derive(Serialize, Deserialize)]
enum FragmentData {
    FileDNode(DNodePlainFileContents),
    DirectoryDNode(DNodeDirectoryContents, Vec<String>),
    ObjSetDNode(ObjSet),
    IndirectBlock(IndirectBlock),
}

impl Debug for FragmentData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FragmentData::FileDNode(_) => write!(f, "File"),
            FragmentData::DirectoryDNode(_, _) => write!(f, "Dir"),
            FragmentData::ObjSetDNode(_) => write!(f, "ObjSet"),
            FragmentData::IndirectBlock(_) => write!(f, "Indirect"),
        }?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Fragment {
    data: FragmentData,
    children: HashSet<[u64; 4]>,
}

impl Debug for Fragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data)?;
        write!(f, "(")?;
        for child in self.children.iter() {
            write!(f, "{:?}, ", child[0])?;
        }
        write!(f, ")")?;
        Ok(())
    }
}

impl From<FragmentData> for Fragment {
    fn from(frag: FragmentData) -> Self {
        Self {
            data: frag,
            children: HashSet::new(),
        }
    }
}

fn main() {
    // NOTE: This was made as quick way to filter and merge outputs from undelete checkpoints
    let mut recovered_fragments1: Vec<([u64; 4], Fragment)> =
        serde_json::from_reader(File::open("undelete-step1-checkpoint-first-50%.json").unwrap())
            .unwrap();
    recovered_fragments1.retain(|(_, f)| matches!(f.data, FragmentData::FileDNode(_)));

    let mut recovered_fragments2: Vec<([u64; 4], Fragment)> =
        serde_json::from_reader(File::open("undelete-step1-checkpoint-upto-74%.json").unwrap())
            .unwrap();
    recovered_fragments2.retain(|(_, f)| matches!(f.data, FragmentData::FileDNode(_)));

    let mut recovered_fragments3: Vec<([u64; 4], Fragment)> =
        serde_json::from_reader(File::open("undelete-step1-checkpoint-upto-78%.json").unwrap())
            .unwrap();
    recovered_fragments3.retain(|(_, f)| matches!(f.data, FragmentData::FileDNode(_)));

    let mut recovered_fragments4: Vec<([u64; 4], Fragment)> =
        serde_json::from_reader(File::open("undelete-step1-checkpoint-upto-100%.json").unwrap())
            .unwrap();
    recovered_fragments4.retain(|(_, f)| matches!(f.data, FragmentData::FileDNode(_)));

    let recovered_fragments: Vec<([u64; 4], Fragment)> = recovered_fragments1
        .into_iter()
        .chain(recovered_fragments2.into_iter())
        .chain(recovered_fragments3.into_iter())
        .chain(recovered_fragments4.into_iter())
        .collect();

    write!(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("undelete-filtered-checkpoint.json")
            .unwrap(),
        "{}",
        &serde_json::to_string(&recovered_fragments.into_iter().collect::<Vec<(_, _)>>()).unwrap()
    )
    .unwrap();
}
