if __name__ == "__main__":
    import runpy

    runpy.run_module("code_scripts.run_all_companies", run_name="__main__")
else:
    from code_scripts.run_all_companies import *  # noqa: F401,F403
