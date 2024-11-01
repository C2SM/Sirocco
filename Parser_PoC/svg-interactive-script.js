function addInteractivity(evt) {

	var svg = evt.target;
    var edges = document.getElementsByClassName('edge');
    var nodes = document.getElementsByClassName('node');
	var clusters = document.getElementsByClassName('cluster');
    var selectedElement, offset, transform, nodrag, origmousepos;

    svg.addEventListener('mousedown', startDrag);
    svg.addEventListener('mousemove', drag);
    svg.addEventListener('mouseup', endDrag);
    svg.addEventListener('mouseleave', endDrag);
    svg.addEventListener('touchstart', startDrag);
    svg.addEventListener('touchmove', drag);
    svg.addEventListener('touchend', endDrag);
    svg.addEventListener('touchleave', endDrag);
    svg.addEventListener('touchcancel', endDrag);

    for (var i = 0; i < edges.length; i++) {
        edges[i].addEventListener('click', clickEdge);
    }

    for (var i = 0; i < nodes.length; i++) {
        nodes[i].addEventListener('click', clickNode);
    }

    addToggleButtons(evt);

    function getMousePosition(evt) {
        var CTM = svg.getScreenCTM();
        if (evt.touches) { evt = evt.touches[0]; }
        return {
            x: (evt.clientX - CTM.e) / CTM.a,
            y: (evt.clientY - CTM.f) / CTM.d
        };
    }

    function startDrag(evt) {
        origmousepos = getMousePosition(evt);
        nodrag=true;
        selectedElement = evt.target.parentElement;
        if (selectedElement){
            offset = getMousePosition(evt);

            // Make sure the first transform on the element is a translate transform
            var transforms = selectedElement.transform.baseVal;

            if (transforms.length === 0 || transforms.getItem(0).type !== SVGTransform.SVG_TRANSFORM_TRANSLATE) {
                // Create an transform that translates by (0, 0)
                var translate = svg.createSVGTransform();
                translate.setTranslate(0, 0);
                selectedElement.transform.baseVal.insertItemBefore(translate, 0);
            }

            // Get initial translation
            transform = transforms.getItem(0);
            offset.x -= transform.matrix.e;
            offset.y -= transform.matrix.f;
		}
    }

    function drag(evt) {
        if (selectedElement) {
            evt.preventDefault();
            var coord = getMousePosition(evt);
            transform.setTranslate(coord.x - offset.x, coord.y - offset.y);
        }
    }

    function endDrag(evt) {
            <!-- comment out the following line if you wnat drags to stay in place, with this line they snap back to their original position after drag end -->
            //if statement to avoid the header section being affected of the translate (0,0)
        if (selectedElement){
			if (selectedElement.classList.contains('header')){
				selectedElement = false;
			} else {
				selectedElement = false;
				transform.setTranslate(0,0);
			}
		}
		var currentmousepos=getMousePosition(evt);
		if (currentmousepos.x===origmousepos.x|currentmousepos.y===origmousepos.y){
			nodrag=true;
		} else {
			nodrag=false;
		}

	}

    function clickEdge() {
        if (nodrag) {
            if (this.classList.contains("edge-highlight")){
                this.classList.remove("edge-highlight");
                this.classList.remove("text-highlight-edges");
            }
            else {
                this.classList.add("edge-highlight");
                this.classList.add("text-highlight-edges");
                animateEdge(this);
            }
        }
    }

    function clickNode() {
        if (nodrag) {
            var nodeName = this.childNodes[1].textContent;
            // Escape special characters in the node name
            var nodeNameEscaped = nodeName.replace(/[-[\]{}()*+!<=:?.\/\\^$|#\s,]/g, '\\$&');

            var patroon = new RegExp('^' + nodeNameEscaped + '->|->' + nodeNameEscaped + '$|' + nodeNameEscaped + '--|--' + nodeNameEscaped + '$')

            if (this.classList.contains("node-highlight")) {
                this.classList.remove("node-highlight");
                this.classList.remove("text-highlight-nodes");
                var edges = document.getElementsByClassName('edge');
                for (var i = 0; i < edges.length; i++) {
                    if (patroon.test(edges[i].childNodes[1].textContent)) {
                        edges[i].classList.remove("edge-highlight");
                        edges[i].classList.remove("text-highlight-edges");
                    }
                }
            } else {
                this.classList.add("node-highlight");
                this.classList.add("text-highlight-nodes");
                var edges = document.getElementsByClassName('edge');
                for (var i = 0; i < edges.length; i++) {
                    if (patroon.test(edges[i].childNodes[1].textContent)) {
                        edges[i].classList.add("edge-highlight");
                        edges[i].classList.add("text-highlight-edges");
                        animateEdge(edges[i]);
                    }
                }
            }
        }
    }

    function animateEdge(edge){
		var path = edge.querySelector('path');
		var polygon = edge.querySelector('polygon');
		var length = path.getTotalLength();
		// Clear any previous transition
		path.style.transition = path.style.WebkitTransition = 'none';
		if (polygon){polygon.style.transition = polygon.style.WebkitTransition = 'none';};
		// Set up the starting positions
		path.style.strokeDasharray = length + ' ' + length;
		path.style.strokeDashoffset = length;
		if(polygon){polygon.style.opacity='0';};
		// Trigger a layout so styles are calculated & the browser
		// picks up the starting position before animating
		path.getBoundingClientRect();
		// Define our transition
		path.style.transition = path.style.WebkitTransition =
		    'stroke-dashoffset 2s ease-in-out';
		if (polygon){polygon.style.transition = polygon.style.WebkitTransition =
		             'fill-opacity 1s ease-in-out 2s';};
		// Go!
		path.style.strokeDashoffset = '0';
        if (polygon){setTimeout(function(){polygon.style.opacity='1';},2000)};
	}
}

var svg = document.querySelector('svg');
var viewBox = svg.viewBox.baseVal;
allShown=true;
function addToggleButtons(evt) {
    var svg = evt.target;
    classArray = [];
    currentClassIndex = 0;

    var uniqueClasses = new Set();
    var elements = document.getElementsByTagName("g");

    for (var i = 0; i < elements.length; i++) {

	    var allclasses = elements[i].getAttribute("class");
	    if (allclasses) {
		    var classes = allclasses.split(" ");
		    for (var j = 0; j < classes.length; j++) {
		        var currentClass = classes[j];
		        if (currentClass !== "graph" && !uniqueClasses.has(currentClass)) {
			        uniqueClasses.add(currentClass);
			        classArray.push(currentClass);
		        }
		    }
	    }
    }
    classArray.sort();

    var buttonContainer = document.createElementNS("http://www.w3.org/2000/svg", "g");
    buttonContainer.setAttribute("transform", "translate(10, -30)");
    buttonContainer.setAttributeNS(null, 'class', 'header');


    var header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.setAttribute("x", 100);
    header.setAttribute("y", -10);
    header.setAttribute("text-anchor", "middle");
    header.setAttribute("font-size", "15px");
    header.setAttribute("fill", "black");
    header.textContent = "Toggle Visibility:";

    var prevButton = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    prevButton.setAttribute("x", 0);
    prevButton.setAttribute("y", 0);
    prevButton.setAttribute("rx", 5);
    prevButton.setAttribute("width", 20);
    prevButton.setAttribute("height", 25);
    prevButton.setAttribute("fill", "lightgray");
    prevButton.setAttribute("stroke", "black");
    prevButton.setAttribute("onclick", "prevClass()");

    var prevButtonText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    prevButtonText.setAttribute("id", "prevButtonText");
    prevButtonText.setAttribute("x", 10);
    prevButtonText.setAttribute("y", 18);
    prevButtonText.setAttribute("text-anchor", "middle");
    prevButtonText.setAttribute("font-size", "20px");
    prevButtonText.setAttribute("fill", "black");
    prevButtonText.setAttribute("onclick", "prevClass()");
    prevButtonText.textContent = "<";

    var nextButton = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    nextButton.setAttribute("x", 300);
    nextButton.setAttribute("y", 0);
    nextButton.setAttribute("rx", 5);
    nextButton.setAttribute("width", 20);
    nextButton.setAttribute("height", 25);
    nextButton.setAttribute("fill", "lightgray");
    nextButton.setAttribute("stroke", "black");
    nextButton.setAttribute("onclick", "nextClass()");

    var nextButtonText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    nextButtonText.setAttribute("id", "nextButtonText");
    nextButtonText.setAttribute("x", 310);
    nextButtonText.setAttribute("y", 18);
    nextButtonText.setAttribute("text-anchor", "middle");
    nextButtonText.setAttribute("font-size", "20px");
    nextButtonText.setAttribute("fill", "black");
    nextButtonText.setAttribute("onclick", "nextClass()");
    nextButtonText.textContent = ">";

    var toggleButton = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    toggleButton.setAttribute("id", "toggleButton");
    toggleButton.setAttribute("x", 30);
    toggleButton.setAttribute("y", 0);
    toggleButton.setAttribute("rx", 5);
    toggleButton.setAttribute("width", 260);
    toggleButton.setAttribute("height", 25);
    toggleButton.setAttribute("fill", "#007bbf");
    toggleButton.setAttribute("stroke", "black");
    toggleButton.setAttribute("onclick", "toggleVisibility(classArray[currentClassIndex])");

    var toggleButtonText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    toggleButtonText.setAttribute("id", "toggleButtonText");
    toggleButtonText.setAttribute("x", 160);
    toggleButtonText.setAttribute("y", 18);
    toggleButtonText.setAttribute("text-anchor", "middle");
    toggleButtonText.setAttribute("font-size", "17px");
    toggleButtonText.setAttribute("fill", "black");
    toggleButtonText.setAttribute("onclick", "toggleVisibility(classArray[currentClassIndex])");
    toggleButtonText.textContent = classArray[currentClassIndex];

    var allButton = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    allButton.setAttribute("id", "allButton");
    allButton.setAttribute("x", 340);
    allButton.setAttribute("y", 0);
    allButton.setAttribute("width", 150);
    allButton.setAttribute("height", 25);
    allButton.setAttribute("fill", "#007bbf");
    allButton.setAttribute("stroke", "black");
    allButton.setAttribute("onclick", "toggleAll()");

    var allButtonText = document.createElementNS("http://www.w3.org/2000/svg", "text");
    allButtonText.setAttribute("id", "allButtonText");
    allButtonText.setAttribute("x", 415);
    allButtonText.setAttribute("y", 18);
    allButtonText.setAttribute("text-anchor", "middle");
    allButtonText.setAttribute("font-size", "20px");
    allButtonText.setAttribute("fill", "black");
    allButtonText.setAttribute("onclick", "toggleAll()");
    allButtonText.textContent = "Hide/show All";

    buttonContainer.appendChild(header);
    buttonContainer.appendChild(prevButton);
    buttonContainer.appendChild(prevButtonText);
    buttonContainer.appendChild(nextButton);
    buttonContainer.appendChild(nextButtonText);
    buttonContainer.appendChild(toggleButton);
    buttonContainer.appendChild(toggleButtonText);
    buttonContainer.appendChild(allButton);
    buttonContainer.appendChild(allButtonText);
    adjustViewBox(svg);
    svg.appendChild(buttonContainer);
    updateButton();
}

function nextClass() {
    currentClassIndex = (currentClassIndex + 1) % classArray.length;
    updateButton();
}

function prevClass() {
    currentClassIndex = (currentClassIndex - 1 + classArray.length) % classArray.length;
    updateButton();
}

function updateButton() {
    var buttonText = document.getElementById("toggleButtonText");
    var button = document.getElementById("toggleButton");
    buttonText.textContent = classArray[currentClassIndex];
    elements=document.getElementsByClassName(classArray[currentClassIndex]);

    if (elements[0].style.visibility === "hidden"){
	    button.setAttribute("fill", "lightblue");
	}
    else{
	    button.setAttribute("fill", "#007bbf");};
}

function toggleVisibility(className) {
    var elements = document.getElementsByClassName(className);


    for (var i = 0; i < elements.length; i++) {
        if (elements[i].style.visibility === "hidden") {
            elements[i].style.visibility = "visible";

        } else {
            elements[i].style.visibility = "hidden";
        }
    }
    updateButton();
}

function toggleAll(){

    for (var i = 0; i < classArray.length; i++) {
        var elements = document.getElementsByClassName(classArray[i]);
        for (var j = 0; j < elements.length; j++) {
            if (allShown) {
                elements[j].style.visibility = "hidden";

            } else {
                elements[j].style.visibility = "visible";
            }
        }

    }
    updateButton();
    //update showall button
    var button = document.getElementById("allButton");
    if (allShown){
	    button.setAttribute("fill", "lightblue");
    }
    else{
        button.setAttribute("fill", "#007bbf");
    }
    allShown = !allShown;
}

function adjustViewBox(svg) {
    var viewBoxParts = svg.getAttribute("viewBox").split(" ");
    var newYMin = parseFloat(viewBoxParts[1]) - 60; // Adjust this value as needed
    var newYMax = parseFloat(viewBoxParts[3]) + 60; // Adjust this value as needed
    var newXMax = Math.max(parseFloat(viewBoxParts[2]),240);
    var newViewBox = viewBoxParts[0] + " " + newYMin + " " + newXMax + " " + newYMax;
    svg.setAttribute("viewBox", newViewBox);
}
