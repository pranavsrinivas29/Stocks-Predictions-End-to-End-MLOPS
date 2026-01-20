import os

# Folders that should be treated as Python packages
PYTHON_DIRS = {
    "app",
    "inference",
    "pipelines",
    "research",
    "tests",
    "training",
    "ui"
}

PROJECT_STRUCTURE = {
    #".dvc": {},
    ".github": {
        "workflows": {}
    },
    "app": {},
    "data": {},
    "docker": {},
    "images": {},
    "inference": {},
    "k8s": {},
    "mlruns": {},
    "pipelines": {},
    "research": {},
    "tests": {},
    "training": {},
    "ui": {},

    # Root-level files
    #".dvcignore": "",
    ".gitignore": "",
    "README.md": "",
    "config.py": "",
    "dvc.yaml": "",
    "dvc.lock": "",
    "params.yaml": "",
    "plan.txt": "",
    "requirements.txt": "",
    "requirements-dev.txt": ""
}


def create_structure(base_path, structure):
    for name, content in structure.items():
        path = os.path.join(base_path, name)

        if isinstance(content, dict):
            os.makedirs(path, exist_ok=True)

            # Auto-create __init__.py for Python packages
            if name in PYTHON_DIRS:
                init_file = os.path.join(path, "__init__.py")
                open(init_file, "a").close()

            create_structure(path, content)

        else:
            with open(path, "a"):
                pass


if __name__ == "__main__":
    project_root = os.getcwd()
    create_structure(project_root, PROJECT_STRUCTURE)
    print("✅ Project structure with __init__.py created successfully.")
