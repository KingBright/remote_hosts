# The shell entry point is loaded by Android app_process after explicit ADB activation.
-keep class io.remotehosts.agent.ShellMain { public static void main(java.lang.String[]); }
-keep class io.remotehosts.agent.ShellMain$* { *; }
