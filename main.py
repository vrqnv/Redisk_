#!/usr/bin/env python3
import sys
import os

def main():
    sys.path.insert(0, os.path.dirname(__file__))
    from gui import main as gui_main
    gui_main()

if __name__ == "__main__":
    main()