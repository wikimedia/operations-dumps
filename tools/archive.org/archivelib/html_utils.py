import re
from archivelib.error import ArchiveUploaderError


def get_login_cookies(text):
    """Get cookie out of the text returned from the
    archive.org login form. gahhhh"""
    # format: .archive.org^ITRUE^I/^IFALSE^I1361562342^Ilogged-in-sig^Isomehugenumberhere
    # .archive.org^ITRUE^I/^IFALSE^I1361562342^Ilogged-in-user^Ijohndoe%40wikimedia.org$
    # plus others which we will ignore.
    cookies = []
    lines = text.split('\n')
    for line in lines:
        if (not len(line)) or (line[0] == '#'):
            continue
        parts = line.split('\t')
        print parts
        if parts[5] == 'logged-in-user':
            cookies.append("%s=%s" % (parts[5], parts[6]))
        elif parts[5] == 'logged-in-sig':
            cookies.append("%s=%s" % (parts[5], parts[6]))
    if len(cookies):
        return '; '.join(cookies)
    else:
        return None


def strip_hidden(cell):
    start = cell.find('<span class="catHidden">')
    if start != -1:
        index = start + 1
        open_tags = 1
        span_open_or_close_expr = re.compile('(<span[^>]+>|</span>)')
        # find index of first occurrence and what was matched,
        # so we can see if it was open or close tag
        while open_tags:
            span_match = span_open_or_close_expr.search(cell[index:])
            if span_match:
                tag_found = span_match.group(1)
                index = span_match.start(1) + index
                if tag_found == '</span>':
                    open_tags = open_tags - 1
                else:
                    open_tags = open_tags + 1
            else:
                # bad html. just toss the rest of the cell
                open_tags = 0
                index = -1
        # now we have the index where we found the close tag for us.
        # toss everything up to that, we'll lose the actual close tag when we
        # toss the rest of the html
        cell = cell[:start] + cell[index:]
    return cell


def show_item_status_from_html(text):
    """Wade through the html output to find information
    about each job we have requested and its status.
    THIS IS UGLY and guaranteed to break in the future
    but hey, there's no json output available, nor xml."""
    html_tag_expr = re.compile('(<[^>]+>)+')
    # get the headers for the table of tasks
    start = text.find('<tr><th><b><a href="/catalog.php?history=1&identifier=')
    if start >= 0:
        end = text.find('<!--task_ids: -->', start)
        content = text[start:end]
        print "content is", content
        lines = content.split('</th>')
        lines = [re.sub(html_tag_expr, '', line).strip()
                 for line in lines if
                 line.find('<th><b><a href="/catalog.php?history=1&identifier=') != -1]
        print ' | '.join(filter(None, lines))

    # get the tasks themselves
    start = text.find('<!--task_ids: -->')
    if start < 0:
        raise ArchiveUploaderError(
            "Can't locate the beginning of the item status information"
            " in the html output.")
    end = text.find('</table>', start)
    content = text[start:end]
    lines = content.split('</tr>')

    for line in lines:
        line = line.replace('\n', '')
        cells = line.split('</td>')
        cells_to_print = [re.sub(html_tag_expr, '',
                                 strip_hidden(cell)).strip()
                          for cell in cells]
        print ' | '.join(filter(None, cells_to_print))
