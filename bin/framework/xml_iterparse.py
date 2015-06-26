from cStringIO import StringIO
import xml.etree.cElementTree as ElementTree


def _do_handlers(evt, elem, current_path, path_handlers):
    handlers_to_be_invoked = []
    for interested_path, handler in path_handlers:
        # interested_path shall be a prefix of current_path
        # interested_path /a/b/c <----> /a/b/c/d/e current_path

        if len(interested_path) > len(current_path):
            continue
        else:
            for i, path in enumerate(interested_path):
                if path != current_path[i]:
                    break
            else:
                handlers_to_be_invoked.append(handler)

    for handler in handlers_to_be_invoked:
        handler(evt, elem, current_path)


def iterparse(text, interested_path_handlers):
    """
    An incremental XML parser. ElementTree.findall has too high CPU/Memory
    footprint when data set is big

    @interested_path_handlers:
      {"start": ((interested_path, handler), (interested_path, handler), ...),
       "end": ((interested_path, handler), (interested_path, handler), ...)}
    interested_path => (tag1, tag2, tag3, ...) which form a XPath
    """

    strf = StringIO()
    strf.write(text)
    strf.seek(0)

    context = ElementTree.iterparse(strf, events=("start", "end"))
    context = iter(context)
    all_start_handlers = interested_path_handlers.get("start", ())
    all_end_handlers = interested_path_handlers.get("end", ())
    current_path = []

    for ev, elem in context:
        tag, value = elem.tag, elem.text
        if ev == "start":
            current_path.append(tag)
            if all_start_handlers:
                _do_handlers(ev, elem, current_path, all_start_handlers)
        elif ev == "end":
            if all_end_handlers:
                _do_handlers(ev, elem, current_path, all_end_handlers)
            current_path.pop()
            elem.clear()
