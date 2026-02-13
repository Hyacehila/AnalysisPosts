$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

# Ensure the project root is on PYTHONPATH so "dashboard" can be imported in Streamlit pages.
$env:PYTHONPATH = $repoRoot

& "$repoRoot\\.venv\\Scripts\\streamlit.exe" run "$repoRoot\\dashboard\\app.py"
