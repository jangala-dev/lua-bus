#!/bin/sh

cd /tmp
git clone https://github.com/jangala-dev/lua-fibers
cd lua-fibers
git checkout v0.1
sudo mv fibers /usr/local/share/lua/5.1/
cd ..
rm -rf lua-fibers

cd /tmp
git clone https://github.com/jangala-dev/lua-trie
cd lua-trie
git checkout v0.1
sudo mv src/trie.lua /usr/local/share/lua/5.1/
cd ..
rm -rf lua-trie

exit 0
