# Renders assets/kill-demo.gif with vhs (https://github.com/charmbracelet/vhs).
# Needs DATABASE_URL in the environment. See docs/kill-demo.md.
#
#   DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres vhs scripts/kill-demo.tape

Output assets/kill-demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1240
Set Height 920
Set Padding 24
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms

Hide
Type "clear" Enter
Show

Sleep 600ms
Type "npm run demo:kill"
Sleep 500ms
Enter
Wait+Screen@60s /total \d+\.\d s/
Sleep 4s
