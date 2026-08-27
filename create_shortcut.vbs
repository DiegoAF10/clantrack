Set WshShell = CreateObject("WScript.Shell")
Set shortcut = WshShell.CreateShortcut(WshShell.SpecialFolders("Desktop") & "\ClanTrack.lnk")
shortcut.TargetPath = WshShell.ExpandEnvironmentStrings("%WINDIR%") & "\System32\wscript.exe"
shortcut.Arguments = Chr(34) & "C:\Users\Diego\projects\clan\coo\walmart\ClanTrack.vbs" & Chr(34)
shortcut.WorkingDirectory = "C:\Users\Diego\projects\clan\coo\walmart"
shortcut.IconLocation = "C:\Users\Diego\projects\clan\coo\walmart\ClanTrack.ico, 0"
shortcut.Description = "ClanTrack — Retail Intelligence"
shortcut.WindowStyle = 7
shortcut.Save
WScript.Echo "Shortcut updated with icon"
