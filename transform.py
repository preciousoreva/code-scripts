if __name__ == "__main__":
    import runpy

    runpy.run_module("code_scripts.transform", run_name="__main__")
else:
    from code_scripts.transform import *  # noqa: F401,F403
