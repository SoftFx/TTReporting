docker compose run --rm --entrypoint sh diff-prices-local -lc "R -q -e 'installed.packages()'" >>customcommand_output.log
pause