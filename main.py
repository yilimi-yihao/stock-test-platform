import sys
from pathlib import Path

# 让根目录直接运行时可以找到 src/sql_tool 包
sys.path.insert(0, str(Path(__file__).parent / "src"))

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "gui":
        from sql_tool.gui.desktop import main as gui_main
        gui_main()
    else:
        from sql_tool.cli import main as cli_main
        cli_main()
