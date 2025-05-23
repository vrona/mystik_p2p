Mystik commands for file storage and retrieving.

call <filename> <.filetype> <secret>

store <filename> <.filetype> <secret> <filepath>

get  <filename> <.filetype> <secret> <private_key> <nonce>

unset STORAGE_DIR; unset STORAGE_MIDCID; unset STORAGE_METADATA

export STORAGE_DIR="./nodeA/content/"; export STORAGE_MIDCID="./nodeA/metadata/midcid/"; export STORAGE_MERKLE="./nodeA/metadata/metamerkle/"; export STORAGE_METADATA="./nodeA/metadata/metamerkle/"
export STORAGE_DIR="./nodeB/content/"; export STORAGE_MIDCID="./nodeB/metadata/midcid/"; export STORAGE_MERKLE="./nodeB/metadata/metamerkle/"; export STORAGE_METADATA="./nodeB/metadata/metamerkle/"
export STORAGE_DIR="./nodeC/content/"; export STORAGE_MIDCID="./nodeC/metadata/midcid/"; export STORAGE_MERKLE="./nodeC/metadata/metamerkle/"; export STORAGE_METADATA="./nodeC/metadata/metamerkle/"
export STORAGE_DIR="./nodeD/content/"; export STORAGE_MIDCID="./nodeD/metadata/midcid/"; export STORAGE_MERKLE="./nodeD/metadata/metamerkle/"; export STORAGE_METADATA="./nodeD/metadata/metamerkle/"

export STORAGE_DIR="./nodeG/content/"; export STORAGE_MIDCID="./nodeG/metadata/midcid/"; export STORAGE_MERKLE="./nodeG/metadata/metamerkle/"; export STORAGE_METADATA="./nodeG/metadata/metamerkle/"