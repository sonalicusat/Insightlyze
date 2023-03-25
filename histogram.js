// Load the CSV data and create the histogram
fetch("/static/histogram.csv")
  .then((response) => response.text())
  .then((csvText) => {
    const rows = csvText.trim().split("\n");
    const headers = rows[0].split(",");
    const data = rows.slice(1).map((row) => {
      const values = row.split(",");
      return {
        Bucket: values[0],
        Rec_Count: +values[1],
      };
    });

    // Create the histogram
    const trace = {
      x: data.map((d) => d.Bucket),
      y: data.map((d) => d.Rec_Count),
      type: "bar",
      marker: {
        color: "steelblue",
      },
    };

    const layout = {
      title: "Histogram",
      xaxis: { title: "Bucket" },
      yaxis: { title: "Rec_Count" },
    };

    Plotly.newPlot("histogram", [trace], layout);
  })
  .catch((error) => {
    console.error("Error loading data:", error);
  });
