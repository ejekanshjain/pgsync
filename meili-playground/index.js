const { MeiliSearch } = require('meilisearch')

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

const main = async () => {
  const client = new MeiliSearch({
    host: 'http://localhost:7700'
  })

  const index = 'products'

  await client.index(index).updateFilterableAttributes(['brands.name'])

  await delay(5000)

  const results = await client.index(index).search('iphone', {
    filter: `brands.name = "Apple"`
  })
  console.log(results)
}

main()
