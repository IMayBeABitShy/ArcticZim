// script for blurring spoilers and NSFW content

async function blur_all_spoilers() {
    // blurr all spoilers
    var posts = document.getElementsByClassName("postbody");
    for (const post of posts) {
        if (post.dataset.spoiler_rendered != true) {
            await handle_blur(post);
            post.dataset.spoiler_rendered = true;
        }
    }
}

// blur the content of a post pody
async function handle_blur(post) {
    var should_blur = (post.dataset.spoiler == "1") || (post.dataset.nsfw == "1");
    if (!should_blur) {
        return;
    }
    post.classList.toggle("blured");
    post.addEventListener(
        "click",
        function() {
            this.classList.toggle("blured");
        }
    );
    // for videos, also remove blur on play
    var videos = post.getElementsByTagName("video");
    for (const video of videos) {
        video.addEventListener(
        "play",
        function() {
            this.parentElement.classList.remove("blured");
        }
    );
    }
}

// observer logic - for dynamic loading of polls via preview
async function spoiler_callback(mutationList, observer) {
    console.log("Spoilers: detected content modification, re-blurring spoilers");
    await blur_all_spoilers();
};


function install_spoiler_mutation_listeners() {
    // called on startup to install mutation listeners, which monitor dynamic loading of posts
    console.log("Spoilers: installing mutation listeners...");
    const observer = new MutationObserver(spoiler_callback);
    var elements = document.getElementsByClassName("postsummary");
    let config = { childList: true, subtree: true };
    for (const element of elements) {
        observer.observe(element, config);
    }
    console.log("Spoilers: successfully installed mutation listeners.");
}

// run logic

async function spoilers_on_load(event) {
    // called when everything was loaded
    await install_spoiler_mutation_listeners();
}

function main() {
    // the main function
    window.addEventListener("load", spoilers_on_load);
}

main();
