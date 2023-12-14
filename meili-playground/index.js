const { MeiliSearch } = require('meilisearch')

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

const main = async () => {
  const client = new MeiliSearch({
    host: 'http://localhost:7700'
  })

  const index = 'Products'

  await client.index(index).updateFilterableAttributes(['Brands.name'])

  await delay(5000)

  const results = await client.index(index).search('iphone', {
    filter: `Brands.name = "Apple"`
  })
  console.log(results)
}

main()
