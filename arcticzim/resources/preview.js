// script for showing preview of posts in arcticzim

function install_handlers() {
    var elements = document.getElementsByClassName("posticon");
    for (const element of elements) {
        element.addEventListener(
            "click",
            function() {
                var post_id = this.parentElement.parentElement.dataset.post;
                var subreddit_name = this.parentElement.parentElement.dataset.subreddit;

                if ($("#preview-" + post_id).length == 0) {
                    // initial fetch
                    var container = $(this.parentElement.parentElement)
                    var content = $('<div id="preview-' + post_id + '"></div>');
                    container.append(content);
                    content.css("display", "block");
                    content.load("../../../r/" + subreddit_name + "/" + post_id + "/ #postbody");
                } else {
                    var content = document.getElementById("preview-" + post_id);
                    if (content.style.display == "block") {
                        content.style.display = "none";
                    } else {
                        content.style.display = "block";
                    }
                }
            }
        );
    }
}

// run logic

function on_load(event) {
    // called when everything was loaded
    install_handlers();
}

function main() {
    // the main function
    window.addEventListener("load", on_load)
}

main();
