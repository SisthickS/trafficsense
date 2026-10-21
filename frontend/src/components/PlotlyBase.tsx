"use client";

import Plotly from "plotly.js-cartesian-dist-min";
import createPlotlyComponent from "react-plotly.js/factory";

// Only the cartesian bundle (scatter, bar, heatmap, histogram, pie ...) is shipped: ~1 MB instead of the full 4 MB.
const Plot = createPlotlyComponent(Plotly);
export default Plot;
