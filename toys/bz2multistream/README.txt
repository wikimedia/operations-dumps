What is this?

It's a proof of concept for the use of multi-stream bz2-compressed
XML files from the wiki projects for offline use.

Let's back that up and try again.

Using a reformatted copy of the XML files containing text of the
articles for your favorite wikiproject, you can run a little script
to retrieve the text of a given article without reading through
the entire file to find it, but by going (almost) directly to
the place in the file with the article and retrieving it.

Another potential use for this format is by researchers, analysts
or contributors who may want to work with specific multiple article texts
at once in an automated fashion.

See INSTALL.txt for how to generated the needed files and how
to run the article text retrieval script.

Platforms:

If you have the index and toc fles generated already,
I suppose the script would probably run on Windows and on MacOS
but it's only tested on Linux.

Caveats:

This is not meant to be anything more than a toy to play with.
If you are interested in serious offline work, these tools
may show you one approach to expand upon.  There are a number
of offline readers availble, all of which do something different:
See OFFLINE.txt for a list of those known to the author as of
September 2012.

License:

Copyright © 2012, Ariel Glenn <ariel@wikimedia.org>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program (see the file COPYING); if not, write to the Free
Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
MA 02110-1301, USA. See also the FSF website,
http://www.gnu.org/copyleft/gpl.html
