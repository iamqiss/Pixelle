#!/bin/bash
Configuration
old_names=("messenger" "maintable")
new_names=("messenger" "maintable")
workspace_path="." # Change this to your root workspace path if it's not the current directory
--- Functions ---
Function to rename files and directories
rename_filesystem() {
local old_name=$1
local new_name=$2
# Escape special characters for sed and find
local escaped_old=$(echo "$old_name" | sed 's/[.()|[]*+?^$]/\\&/g')
local escaped_new=$(echo "$new_name" | sed 's/[.()|[]*+?^$]/\\&/g')

echo "Renaming files and directories from '$old_name' to '$new_name'..."

# Find and rename directories
find "$workspace_path" -depth -type d -name "*$old_name*" -print0 | while IFS= read -r -d '' dir; do
    mv "$dir" "$(echo "$dir" | sed -E "s/$escaped_old/$escaped_new/gi")"
done

# Find and rename files
find "$workspace_path" -depth -type f -name "*$old_name*" -print0 | while IFS= read -r -d '' file; do
    mv "$file" "$(echo "$file" | sed -E "s/$escaped_old/$escaped_new/gi")"
done

}
Function to update file contents
update_file_contents() {
local old_name=$1
local new_name=$2
echo "Updating contents inside files from '$old_name' to '$new_name'..."

# Use grep to find files containing the old name (case-insensitive)
# and then use sed to perform a case-insensitive replacement
grep -rlI "$old_name" "$workspace_path" | while IFS= read -r file; do
    sed -i '' -E "s/$old_name/$new_name/gi" "$file"
done

}
--- Main Script ---
echo "Starting repository and content renaming process..."
Loop through each pair of names to perform the renaming
for ((i=0; i<{\#old\_names[@]}; i++)); do
old\_name={old_names[i]}
new\_name={new_names[$i]}
echo "--------------------------------------------------------"
echo "Processing: $old_name -> $new_name"

# Step 1: Update file contents
update_file_contents "$old_name" "$new_name"

# Step 2: Rename files and directories
rename_filesystem "$old_name" "$new_name"

echo "Completed processing for $old_name."

done
echo "--------------------------------------------------------"
echo "Renaming process completed successfully! ✨"

## How the script works 🛠️
***
The script uses a combination of standard command-line tools like `find`, `grep`, `sed`, and `mv` to perform the renaming operations.

### Configuration
The script starts by defining two arrays: `old_names` and `new_names`. These arrays hold the **source** and **target** names, respectively. The `workspace_path` variable is set to the current directory (`.`) but can be changed to point to a specific root workspace.

### Functions
* **`rename_filesystem`**: This function handles the renaming of directories and files.
    * It uses **`find`** to locate directories and files with names containing the `old_name`. The `-depth` flag is crucial as it ensures directories are renamed from the bottom up, preventing errors where a parent directory is renamed before its contents.
    * The `find` command passes the list of found paths to a `while` loop using `print0` and `read -r -d ''` to handle paths with spaces or other special characters correctly.
    * **`mv`** is used to perform the actual renaming, with a nested `sed` command to replace the old name with the new one. The `-E` flag in `sed` enables extended regular expressions, and the `/gi` flags make the substitution global and **case-insensitive**.
* **`update_file_contents`**: This function updates the text inside the files.
    * **`grep -rlI`** is used to find files containing the `old_name`.
        * `-r`: recursive search
        * `-l`: lists the files that match, instead of printing the matching lines
        * `-I`: ignores binary files
    * The list of files from `grep` is then piped to a `while` loop.
    * **`sed -i ''`** performs an in-place edit of the files. The `''` after `-i` is specific to macOS and prevents `sed` from creating a backup file. If you're using Linux, you may need to remove this or use `-i''` without the space. The `s/old_name/new_name/gi` command performs a case-insensitive and global substitution of the old name with the new name.

### Main Logic
The script loops through the `old_names` and `new_names` arrays. For each pair, it first calls `update_file_contents` and then `rename_filesystem`. This order is important: if the files were renamed first, the `grep` command might not find them in the next step.

