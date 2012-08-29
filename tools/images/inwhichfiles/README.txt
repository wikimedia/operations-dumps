This tools is likely of no use to anyone else in the world
but I'm stashing it here so it doesn't get lost when my
laptop hd and all my backups die horribly some day.

I used it to get a sense of which remotely stored media files were
in use on how many projects, how much overlapping there was,
which projects were the big media users, etc.

This program expects as input a bunch of (gzipped or not)
files containing all the media in use on a given project,
where the filenames look like e.g.

elwikiversity-20120801-remote-wikiqueries.gz

starting with the name of the project and a hyphen and then any random
kind of crap, but ending in .gz if it's gzipped,
and the contents of the files look like

UIG0.jpg	 20091221151253
Science-symbol-2.svg	20080214210816
Ομοιότητα_σε_διανύσματα.PNG	20100421182316

with a tab separator after the name of the file and any
other random crap afterwards.

It runs on linux and freebsd.  And probably nowhere else.
