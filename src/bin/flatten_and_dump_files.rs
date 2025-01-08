use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// We'll store the flattened output files in this folder.
static OUTPUT_DIR: &str = "output_files";

/// Maximum lines per flattened output file before rolling over.
static MAX_LINES_PER_FILE: usize = 500;

fn main() {
    // 1. Get the project directory from command line argument
    let project_dir = std::env::args()
        .nth(1)
        .expect("Usage: cargo run --bin flatten_rust_files -- <path_to_rust_project>");

    println!("Flattening all .rs files from:\n  {}", project_dir);

    // 2. Create the output folder (if not already existing)
    fs::create_dir_all(OUTPUT_DIR).expect("Failed to create output_files directory");

    // 3. Initialize our aggregator
    let mut aggregator = LineAggregator::new();

    // 4. Flatten .rs files in src/, tests/, examples/ 
    flatten_rust_files(&mut aggregator, &project_dir, "src");
    flatten_rust_files(&mut aggregator, &project_dir, "tests");
    flatten_rust_files(&mut aggregator, &project_dir, "examples");

    // 5. Ensure we finalize the last opened file (if any)
    aggregator.close_file();

    println!("\nAll done! Check the '{}' folder for flattened files.", OUTPUT_DIR);
}

/// Recursively look for .rs files in the subfolder of the given dir (e.g., src, tests, examples),
/// and flatten each into the aggregator.
fn flatten_rust_files(aggregator: &mut LineAggregator, dir: &str, subfolder: &str) {
    let path = Path::new(dir).join(subfolder);
    if path.exists() && path.is_dir() {
        println!("  Searching .rs files under: {}", path.display());

        for entry in WalkDir::new(&path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
        {
            if let Some(ext) = entry.path().extension() {
                if ext == "rs" {
                    println!("    Flattening {}", entry.path().display());
                    aggregator.write_file_header(entry.path());
                    aggregator.write_file_contents(entry.path());
                }
            }
        }
    }
}

/// A helper that manages writing lines to the current "flattened" output file,
/// and rolling over to a new file after hitting the max line count.
struct LineAggregator {
    current_file: Option<File>,
    file_index: usize,
    line_count_in_current: usize,
}

impl LineAggregator {
    /// Create a new aggregator with no open file yet.
    fn new() -> Self {
        LineAggregator {
            current_file: None,
            file_index: 0,
            line_count_in_current: 0,
        }
    }

    /// Write a special comment line indicating the file we are about to dump.
    fn write_file_header(&mut self, path: &Path) {
        // Attempt to convert to a relative path from the current directory (for clarity).
        let file_path_str = path
            .strip_prefix(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()))
            .unwrap_or(path)
            .display()
            .to_string();

        // E.g. `// FILE: src/main.rs`
        self.write_line(&format!("// FILE: {}\n", file_path_str));
    }

    /// Read the file line by line and write each line to the aggregator.
    fn write_file_contents(&mut self, path: &Path) {
        match File::open(path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                for line_result in reader.lines() {
                    match line_result {
                        Ok(line) => {
                            self.write_line(&format!("{}\n", line));
                        }
                        Err(e) => {
                            let err_msg = format!("// ERROR READING LINE in {}: {:?}", path.display(), e);
                            self.write_line(&format!("{}\n", err_msg));
                        }
                    }
                }
            }
            Err(e) => {
                // If we fail to open the file, at least log it in the output
                let err_msg = format!("// ERROR OPENING {}: {:?}", path.display(), e);
                self.write_line(&format!("{}\n", err_msg));
            }
        }
    }

    /// Write a single line to the aggregator, rolling over to a new file if necessary.
    fn write_line(&mut self, line: &str) {
        if self.current_file.is_none() {
            self.open_new_file();
        }

        // We assume `unwrap()` is safe here because we call `open_new_file()` above.
        let file = self.current_file.as_mut().unwrap();

        if let Err(e) = file.write_all(line.as_bytes()) {
            eprintln!("Failed to write line to file: {:?}", e);
        }
        self.line_count_in_current += 1;

        // Check if we need to roll over
        if self.line_count_in_current >= MAX_LINES_PER_FILE {
            self.close_file();
            // Next write call will open a new file automatically
        }
    }

    /// Close the current file if it's open.
    fn close_file(&mut self) {
        if let Some(file) = &self.current_file {
            if let Err(e) = file.sync_all() {
                eprintln!("Error syncing file: {:?}", e);
            }
        }
        self.current_file = None;
        self.line_count_in_current = 0;
    }

    /// Open a new flattened file in the output directory.
    fn open_new_file(&mut self) {
        self.file_index += 1;
        let file_name = format!("flattened_{:04}.txt", self.file_index); // e.g. flattened_0001.txt
        let dest_path = Path::new(OUTPUT_DIR).join(file_name);

        println!("    -> Opening new output file: {}", dest_path.display());

        match File::create(&dest_path) {
            Ok(file) => {
                self.current_file = Some(file);
                self.line_count_in_current = 0;
            }
            Err(e) => {
                eprintln!("ERROR creating {}: {:?}", dest_path.display(), e);
                self.current_file = None;
            }
        }
    }
}
