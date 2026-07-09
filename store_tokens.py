if __name__ == "__main__":
    import runpy

    runpy.run_module("code_scripts.store_tokens", run_name="__main__")
else:
    from code_scripts.store_tokens import *  # noqa: F401,F403
