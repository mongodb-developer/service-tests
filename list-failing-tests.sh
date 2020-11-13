#!/usr/bin/env bash
echo "Failing tests"

for f in $(find result* -name "*json"); do
  echo "==> $f"
  cat "$f" | python3 -m json.tool | grep 'status": "fail' -A5 | grep test | sed 's/          "test_file": "/- /g' | sed 's/"//g'
  echo ""
done
