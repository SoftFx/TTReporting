docker compose run --rm --entrypoint sh big-trades-local -lc "R -q -e 'installed.packages()'" >>customcommand_output.log
pause