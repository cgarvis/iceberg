%{
  configs: [
    %{
      name: "default",
      strict: true,
      checks: %{
        enabled: [
          # Adjust max nesting depth to 3 to accommodate lazy Logger evaluation
          # Logger calls wrapped in anonymous functions add one nesting level
          {Credo.Check.Refactor.Nesting, [max_nesting: 3]}
        ]
      }
    }
  ]
}
