// https://github.com/BartBrood/Interactive-Graphviz-Diagrams

// MIT License

// Copyright (c) 2024 BartBrood

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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

    var svg = document.querySelector('svg');
    var viewBox = svg.viewBox.baseVal;
    adjustViewBox(svg);

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
            //if statement to avoid the header section being affected by the translate (0,0)
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
                resetEdge(this); // Reset styles when unclicking
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
                        resetEdge(edges[i]); // Reset styles when unclicking node
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
        
        // Save initial values if they haven't been saved yet
        if (!path.hasAttribute('data-orig-dasharray')) {
            var currentDash = window.getComputedStyle(path).strokeDasharray;
            path.setAttribute('data-orig-dasharray', currentDash || 'none');
        }

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
        if (polygon){
            // Clear previous timeout reference if any to avoid animation conflicts
            if(edge.polygonTimeout) { clearTimeout(edge.polygonTimeout); }
            edge.polygonTimeout = setTimeout(function(){polygon.style.opacity='1';},2000);
        }
    }

    function resetEdge(edge){
        var path = edge.querySelector('path');
        var polygon = edge.querySelector('polygon');
        
        // Clear active timeouts
        if(edge.polygonTimeout) { 
            clearTimeout(edge.polygonTimeout); 
            edge.polygonTimeout = null;
        }

        // Remove inline styles completely to fall back to CSS stylesheet defaults
        path.style.transition = path.style.WebkitTransition = '';
        path.style.strokeDashoffset = '';
        
        // Reapply saved original dasharray value
        if (path.hasAttribute('data-orig-dasharray')) {
            var origDash = path.getAttribute('data-orig-dasharray');
            path.style.strokeDasharray = (origDash === 'none') ? '' : origDash;
        } else {
            path.style.strokeDasharray = '';
        }

        if (polygon) {
            polygon.style.transition = polygon.style.WebkitTransition = '';
            polygon.style.opacity = '';
        }
    }
}

function adjustViewBox(svg) {
    var viewBoxParts = svg.getAttribute("viewBox").split(" ");
    var newYMin = parseFloat(viewBoxParts[1]) - 30; // Adjust this value as needed
    var newYMax = parseFloat(viewBoxParts[3]) + 30; // Adjust this value as needed
    var newXMax = Math.max(parseFloat(viewBoxParts[2]),240);
    var newViewBox = viewBoxParts[0] + " " + newYMin + " " + newXMax + " " + newYMax;
    svg.setAttribute("viewBox", newViewBox);
}
