#!/usr/bin/env bash
set -euo pipefail

echo "Starting nextclade version checker..."

# Download nextclade if not exists
if ! command -v nextclade &> /dev/null; then
   echo "Downloading nextclade..."
   curl -fsSL "https://github.com/nextstrain/nextclade/releases/latest/download/nextclade-x86_64-unknown-linux-gnu" -o "nextclade"
   chmod +x nextclade
   NEXTCLADE="./nextclade"
else
   echo "Using existing nextclade installation"
   NEXTCLADE="nextclade"
fi

# Get datasets and check versions
echo "Fetching latest dataset information..."
$NEXTCLADE dataset list --json > nextclade-datasets.json

echo "Checking dataset versions..."
python nextclade_version.py nextclade-datasets.json

echo "Cleaning up..."
rm nextclade-datasets.json
rm nextclade

echo "Done! Please check outputs in nextclade_versions.csv"