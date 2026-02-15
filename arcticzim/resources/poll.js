// script for rendering poll charts

async function render_all_polls() {
    // render all polls
    var elements = document.getElementsByClassName("poll");
    for (const element of elements) {
        if (element.dataset.rendered != true) {
            await handle_poll(element);
            element.dataset.rendered = true;
        }
    }
}

async function handle_poll(element) {
    // render a specific poll
    var poll_url = element.dataset.polllocation;
    console.log("Fetching poll data...")
    var response = await fetch(poll_url);
    if (!response.ok) {
        // file not found
        console.log("HTTP error " + response.status);
        return false;
    }
    try {
        var dataset = await response.json();
    } catch {
        // invalid json
        console.log("Error reading poll data file!");
        return false;
    }
    console.log("Successfully retrieved poll data.");
    // get the canvas element
    var canvas = element.getElementsByClassName("poll-canvas")[0];
    // create the poll
    new Chart(
        canvas,
        {
            type: "pie",
            responsive: true,
            data: dataset,
            plugins: {
                legend: {
                    position: "bottom",
                },
                title: {
                    display: true,
                    text: "Poll",
                },
            },
        }
    );
}

// observer logic - for dynamic loading of polls via preview
async function chart_callback(mutationList, observer) {
    console.log("detected content modification, re-rendering charts");
    await render_all_polls();
};


function install_mutation_listeners() {
    // called on startup to install mutation listeners, which monitor dynamic loading of charts
    console.log("Installing mutation listeners...");
    const observer = new MutationObserver(chart_callback);
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
    await render_all_polls();
    await install_mutation_listeners();
}

function main() {
    // the main function
    window.addEventListener("load", on_load)
}

main();
