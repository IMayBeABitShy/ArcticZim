// script for blurring spoilers and NSFW content

async function blurr_all_spoilers() {
    // blurr all spoilers
    var posts = document.getElementsByClassName("postbody");
    for (const post of posts) {
        if (post.dataset.spoiler_rendered != true) {
            await handle_blur(post);
            element.dataset.spoiler_rendered = true;
        }
    }
}

// blur the content of a post pody
async function handle_blur(post) {
    var should_blur = post.dataset.spoiler || post.dataset.nsfw;
    if (!should_blur) {
        return;
    }
    post.classList.toggle("blured");
    post.addEventListener(
        "click",
        function() {
            this.classList.toggle("blured");
        }
    };

}

// observer logic - for dynamic loading of polls via preview
async function chart_callback(mutationList, observer) {
    console.log("detected content modification, re-blurring spoilers");
    await blurr_all_spoilers();
};


function install_mutation_listeners() {
    // called on startup to install mutation listeners, which monitor dynamic loading of posts
    console.log("Installing mutation listeners...");
    const observer = new MutationObserver(spoiler_callback);
    var elements = document.getElementsByClassName("postsummary");
    let config = { childList: true, subtree: true };
    for (const element of elements) {
        observer.observe(element, config);
    }
    console.log("successfully installed mutation listeners.");
}

// run logic

async function on_load(event) {
    // called when everything was loaded
    await install_mutation_listeners();
}

function main() {
    // the main function
    window.addEventListener("load", on_load);
}

main();
