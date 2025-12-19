"""Check the actual bytes stored in the database"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

query = """
SELECT notes
FROM fact_demographics
WHERE indicator_id = 9
AND notes LIKE '%ff. Verwaltung%'
LIMIT 1
"""

results = db.execute_query(query)

if results:
    notes = results[0]['notes']

    print("=== Database Encoding Analysis ===\n")
    print(f"Notes string: {repr(notes)}")
    print(f"Length: {len(notes)}")

    # Find the position of 'ff'
    idx = notes.find('ff. Verwaltung')
    if idx > 0:
        # Get the characters around it
        sample = notes[max(0, idx-3):idx+20]
        print(f"\nSample around 'ff': {repr(sample)}")
        print(f"Characters: {[c for c in sample[:10]]}")
        print(f"Hex codes: {[hex(ord(c)) for c in sample[:10]]}")

        # Check what character is before 'ff'
        char_before = notes[idx-1]
        print(f"\nCharacter before 'ff': {repr(char_before)}")
        print(f"Unicode code point: U+{ord(char_before):04X}")

        # Check if it's the correct Ö (U+00D6) or replacement character (U+FFFD)
        if ord(char_before) == 0xFFFD:
            print("❌ It's the Unicode replacement character (U+FFFD)")
            print("   This means the original bytes were invalid UTF-8")
        elif ord(char_before) == 0x00D6:
            print("✓ It's the correct Ö character (U+00D6)")
        elif ord(char_before) == 0xC3:
            print("⚠ It's 0xC3 - this is half of a UTF-8 encoded Ö")
            print("  The data might be double-encoded")
        else:
            print(f"? Unknown character: {repr(char_before)}")

        # Try decoding as different encodings
        print("\n=== Attempting different interpretations ===")

        # If it's stored as latin-1 interpretation of UTF-8 bytes
        try:
            # Ö in UTF-8 is bytes C3 96
            # If these bytes were interpreted as latin-1, they'd be Ã–
            if 'Ã\x96ff' in notes or 'Ã–ff' in notes:
                print("Found double-encoding pattern!")
                # Try to fix it
                fixed = notes.encode('latin-1').decode('utf-8')
                print(f"Fixed version: {fixed[:100]}")
        except:
            pass

db.close()
