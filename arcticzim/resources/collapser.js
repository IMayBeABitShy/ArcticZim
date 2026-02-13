// script for collapsing posts and comments in arcticzim

function install_handlers() {
    var elements = document.getElementsByClassName("collapser");
    for (const element of elements) {
        element.addEventListener(
            "click",
            function() {
                this.parentElement.classList.toggle("inactive-commenttitle");
                this.parentElement.classList.toggle("commenttitle");
                var content = this.parentElement.nextElementSibling;
                if (content.style.display != "none") {
                    content.style.display = "none";
                    this.textContent = "[+]";
                } else {
                    content.style.display = "block";
                    this.textContent = "[-]";
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
