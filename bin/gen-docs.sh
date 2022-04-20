#!/bin/bash
M=connection
pydoc-markdown -I dbhelper -m "$M" --render-toc > "docs/$M.md"
M=csv
pydoc-markdown -I dbhelper -m "$M" --render-toc > "docs/$M.md"
M=dataframe
pydoc-markdown -I dbhelper -m "$M" --render-toc > "docs/$M.md"
M=parquet
pydoc-markdown -I dbhelper -m "$M" --render-toc > "docs/$M.md"
M=vertica
pydoc-markdown -I dbhelper -m "$M" --render-toc > "docs/$M.md"