echo "Current build version $1"
file_suffix="-py3-none-any"
new_file_suffix="-$1-py3-none-any"
for file in dist/*; do
  if [[ "$file" == *"$file_suffix" ]]; then
  file_with_build_version="${file/$file_suffix/$new_file_suffix}"
  echo "Trying to rename: $file to $file_with_build_version"
  mv -v "$file" "$file_with_build_version"
  echo "Renamed $file to $file_with_build_version"
  fi
done