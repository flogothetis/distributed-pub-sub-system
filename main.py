with open("rom.coe", "w") as f:
    f.write("memory_initialization_radix=2;\n")
    f.write("memory_initialization_vector=\n")

    # 3 initial zero lines
    for _ in range(3):
        f.write("00000000000000000000000000000000,\n")

    # Your 5 custom instructions
    instructions = [
        "11100000000000010000000000000110",
        "11100000000000100000000000000110",
        "10000000001000110001000000110000",
        "01000100001000100000000000000001",
        "01000000001000100000000000000000",
    ]
    for instr in instructions:
        f.write(instr + ",\n")

    # Fill with zeros until 1024 total lines
    total_lines = 1024
    lines_so_far = 3 + len(instructions)
    for i in range(total_lines - lines_so_far):
        if i == (total_lines - lines_so_far - 1):
            f.write("00000000000000000000000000000000;\n")  # last line ends with ;
        else:
            f.write("00000000000000000000000000000000,\n")
