docker compose run --rm --entrypoint sh big-deposits-local -lc "R -q -e 'installed.packages()'" >>customcommand_output.log
pause