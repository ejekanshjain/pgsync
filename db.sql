CREATE TABLE brands (
    id text,
    name text NOT NULL,
    "isActive" boolean NOT NULL DEFAULT TRUE,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE products (
    id text,
    name text NOT NULL,
    "isActive" boolean NOT NULL DEFAULT TRUE,
    price double precision NOT NULL,
    popularity integer,
    "brandId" text,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    FOREIGN KEY ("brandId") REFERENCES public.brands (id) ON DELETE CASCADE
);
