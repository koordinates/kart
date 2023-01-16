add_executable(kart_cli_helper kart.c cJSON.c)

set_property(TARGET kart_cli_helper PROPERTY OUTPUT_NAME kart)
set_property(TARGET kart_cli_helper PROPERTY C_STANDARD 11)

target_compile_definitions(kart_cli_helper PRIVATE "$<$<CONFIG:DEBUG>:DEBUG>")
