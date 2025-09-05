def copy_file(src, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    with (
        open(src, "r") as input,
        open(target, "w") as output,
    ):
        output.write(input.read())
