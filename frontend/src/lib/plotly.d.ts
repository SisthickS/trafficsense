declare module "plotly.js-cartesian-dist-min";
declare module "react-plotly.js/factory" {
  import type { ComponentType } from "react";
  const createPlotlyComponent: (plotly: unknown) => ComponentType<any>;
  export default createPlotlyComponent;
}
