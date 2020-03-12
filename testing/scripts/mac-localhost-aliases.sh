#!/usr/bin/env bash
tmpfile=$TMPDIR/localhost-aliases-root.sh
tmpfile2=$TMPDIR/localhost-aliases-root2.sh

pre="alias"
post=" up"
if [[ "$1" == "-" ]] ; then
    pre="-alias"
    post=""
fi

echo "#!/bin/bash" > "$tmpfile"
for dc in $(seq 0 15); do
  for rack in $(seq 0 2); do
    for node in $(seq 0 2); do
      echo "ifconfig lo0 ${pre} 127.${dc}.${rack}.${node}${post}" >> "$tmpfile"
    done
  done
done
grep -v 127.0.0.1 "$tmpfile" | grep -v 127.0.0.0 > "$tmpfile2"
chmod 755 "$tmpfile2"
sudo "$tmpfile2"
rm -f "$tmpfile" "$tmpfile2"
