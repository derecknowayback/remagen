import os
import re
import subprocess

def is_git_repository():
    """检查当前目录是否是Git仓库"""
    try:
        subprocess.run(['git', 'rev-parse', '--is-inside-work-tree'], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False

def get_all_files():
    """获取Git仓库中的所有文件"""
    result = subprocess.run(['git', 'ls-files'], stdout=subprocess.PIPE, text=True)
    return result.stdout.splitlines()

def find_chinese_in_file(file_path):
    """检查文件中是否包含中文字符"""
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
    matches = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, start=1):
                match = chinese_pattern.search(line)
                if match:
                    matches.append((line_number, match.group()))
    except (UnicodeDecodeError, FileNotFoundError):
        pass  # 跳过无法解码或不存在的文件
    return matches

def main():
    if not is_git_repository():
        print("当前目录不是一个Git仓库。")
        return

    files = get_all_files()
    if not files:
        print("未找到任何文件。")
        return

    found = False
    for file in files:
        matches = find_chinese_in_file(file)
        if matches:
            found = True
            print(f"文件: {file}")
            for line_number, chinese_text in matches:
                print(f"  行: {line_number}, 中文: {chinese_text}")

    if not found:
        print("未找到包含中文字符的文件。")

if __name__ == "__main__":
    main()
