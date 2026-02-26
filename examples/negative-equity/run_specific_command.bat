docker compose run --rm --entrypoint sh negative-equity-local -lc "R -q -e 'installed.packages()'" >>customcommand_output.log
pause