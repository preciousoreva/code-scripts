if __name__ == "__main__":
    import runpy

    runpy.run_module("code_scripts.qbo_upload", run_name="__main__")
else:
    from code_scripts.qbo_upload import *  # noqa: F401,F403
