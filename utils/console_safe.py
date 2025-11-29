"""
控制台安全输出工具
解决Windows控制台编码问题
"""

def safe_print(text):
    """安全打印，避免编码错误"""
    try:
        print(text)
    except UnicodeEncodeError:
        # 如果有编码错误，用ascii替换
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text)

def format_status_indicator(status, success_symbol="[OK]", error_symbol="[X]"):
    """格式化状态指示器，避免特殊字符"""
    if status:
        return success_symbol
    else:
        return error_symbol