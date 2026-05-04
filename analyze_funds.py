import json

# Serbest fon listesi
with open(
    r"c:\Users\semudu\AppData\Roaming\Code\User\workspaceStorage\0487c91384c58bfe0b0c8964c2c61348\GitHub.copilot-chat\chat-session-resources\227b9976-6d1e-4c8d-ae53-b0d2ec561d96\toolu_011PXf4Z7jM1KCP3kt1UuWNu__vscode-1777748401089\content.json",
    encoding="utf-8",
) as f:
    data = json.load(f)

funds = data.get("funds", [])
print(f"Toplam serbest fon: {len(funds)}")

# En büyük 25 fon (portföy büyüklüğüne göre)
top = sorted(funds, key=lambda x: x.get("portfolio_size", 0), reverse=True)[:25]
print("\n--- EN BÜYÜK 25 SERBEST FON ---")
for x in top:
    size_b = x["portfolio_size"] / 1e9
    print(f"{x['fund_code']:6s}  {size_b:8.2f}B TL   {x['title'][:65]}")

# Borçlanma araçları fon listesi
with open(
    r"c:\Users\semudu\AppData\Roaming\Code\User\workspaceStorage\0487c91384c58bfe0b0c8964c2c61348\GitHub.copilot-chat\chat-session-resources\227b9976-6d1e-4c8d-ae53-b0d2ec561d96\toolu_01WaYjoGjGNd7uPs5Qpp32oZ__vscode-1777748401086\content.json",
    encoding="utf-8",
) as f:
    data2 = json.load(f)

funds2 = data2.get("funds", [])
print(f"\nToplam borçlanma araçları fon: {len(funds2)}")
top2 = sorted(funds2, key=lambda x: x.get("portfolio_size", 0), reverse=True)[:15]
print("\n--- EN BÜYÜK 15 BORÇLANMA ARAÇLARI FONU ---")
for x in top2:
    size_b = x["portfolio_size"] / 1e9
    print(f"{x['fund_code']:6s}  {size_b:8.2f}B TL   {x['title'][:65]}")
