import re
try:
    with open('siac_home.html', encoding='utf-8') as f:
        content = f.read()
    scripts = re.findall(r'<script\s+[^>]*src=["\'](.*?)["\']', content)
    print("\n".join(scripts))
except Exception as e:
    print(e)
