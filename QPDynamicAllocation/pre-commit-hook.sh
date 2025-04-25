#!/usr/bin/env bash


BRANCH=$(git symbolic-ref HEAD | sed -e 's,.*/\(.*\),\1,')
PROTECTED_BRANCHES="^(master|develop)$"

if [[ "$BRANCH" =~ $PROTECTED_BRANCHES ]]; then
  echo "❌ Direct commits to $BRANCH branch are not allowed."
  exit 1
fi

# Run pytest
echo "✅ Running regression tests..."
python -m pytest QPDynamicAllocation/regression_test.py

# If pytest returns non-zero (test failures), prevent the commit
if [ $? -ne 0 ]; then
    echo "❌ Tests failed. Aborting commit."
    exit 1
fi

echo "✅ Tests passed. Proceeding with commit."
exit 0
 