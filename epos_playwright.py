if __name__ == "__main__":
    import runpy

    runpy.run_module("code_scripts.epos_playwright", run_name="__main__")
else:
    from code_scripts.epos_playwright import *  # noqa: F401,F403
